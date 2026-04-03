import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Update

from bot.middlewares.i18n import JsonI18n
from bot.services.redis_service import RedisService
from config.settings import Settings


@dataclass(frozen=True)
class CallbackGuardRule:
    prefixes: tuple[str, ...]
    rate_scope: str
    max_requests: int
    window_seconds: int
    lock_scope: Optional[str] = None
    lock_ttl_seconds: int = 0


class CallbackGuardMiddleware(BaseMiddleware):
    """
    Technical anti-spam middleware for callback queries.
    - Short rate limits for burst protection.
    - Optional short distributed locks for anti-duplicate processing.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        i18n_instance: JsonI18n,
        redis_service: Optional[RedisService] = None,
    ):
        super().__init__()
        self.settings = settings
        self.i18n_instance = i18n_instance
        self.redis_service = redis_service
        self.rules = (
            CallbackGuardRule(
                prefixes=(
                    "pay_yk:",
                    "pay_yk_new:",
                    "pay_yk_use_saved:",
                    "pay_fk:",
                    "pay_platega:",
                    "pay_severpay:",
                    "pay_crypto:",
                    "pay_stars:",
                ),
                rate_scope="payment_create",
                max_requests=8,
                window_seconds=30,
                lock_scope="payment_create",
                lock_ttl_seconds=max(1, int(self.settings.REDIS_PAYMENT_LOCK_TTL_SECONDS)),
            ),
            CallbackGuardRule(
                prefixes=("pay_yk_saved_list:",),
                rate_scope="payment_saved_cards",
                max_requests=20,
                window_seconds=30,
            ),
            CallbackGuardRule(
                prefixes=("pm:bind",),
                rate_scope="payment_method_bind",
                max_requests=3,
                window_seconds=60,
                lock_scope="payment_method_bind",
                lock_ttl_seconds=max(1, int(self.settings.REDIS_PAYMENT_LOCK_TTL_SECONDS)),
            ),
            CallbackGuardRule(
                prefixes=("trial_action:confirm_activate", "main_action:request_trial"),
                rate_scope="trial_activate",
                max_requests=3,
                window_seconds=60,
                lock_scope="trial_activate",
                lock_ttl_seconds=max(1, int(self.settings.REDIS_PAYMENT_LOCK_TTL_SECONDS)),
            ),
            CallbackGuardRule(
                prefixes=("toggle_autorenew:", "autorenew:confirm:", "autorenew:cancel"),
                rate_scope="autorenew_toggle",
                max_requests=6,
                window_seconds=30,
                lock_scope="autorenew_toggle",
                lock_ttl_seconds=10,
            ),
            CallbackGuardRule(
                prefixes=("main_action:my_subscription", "main_action:my_devices", "disconnect_device:"),
                rate_scope="subscription_open",
                max_requests=12,
                window_seconds=20,
            ),
        )

    async def __call__(
        self,
        handler: Callable[[Update, Dict[str, Any]], Awaitable[Any]],
        event: Update,
        data: Dict[str, Any],
    ) -> Any:
        callback: Optional[CallbackQuery] = event.callback_query
        if (
            callback is None
            or callback.data is None
            or self.redis_service is None
            or not self.redis_service.is_available()
        ):
            return await handler(event, data)

        rule = self._find_rule(callback.data)
        if rule is None:
            return await handler(event, data)

        user_id = callback.from_user.id if callback.from_user else None
        if user_id is None:
            return await handler(event, data)

        rate_limit_key = f"rate_limit:{rule.rate_scope}:{user_id}"
        allowed = await self.redis_service.check_rate_limit(
            rate_limit_key,
            max_requests=rule.max_requests,
            window_seconds=rule.window_seconds,
        )
        if not allowed:
            await self._answer_blocked(callback, data)
            return None

        lock_key = None
        lock_token = None
        if rule.lock_scope:
            lock_key = f"lock:{rule.lock_scope}:{user_id}"
            lock_token = await self.redis_service.acquire_lock(lock_key, rule.lock_ttl_seconds)
            if not lock_token:
                lock_exists = await self.redis_service.exists(lock_key)
                if lock_exists:
                    await self._answer_blocked(callback, data)
                    return None
                lock_key = None

        try:
            return await handler(event, data)
        finally:
            if lock_key and lock_token:
                released = await self.redis_service.release_lock(lock_key, lock_token)
                if not released:
                    logging.debug("CallbackGuardMiddleware: lock was not released for key %s", lock_key)

    def _find_rule(self, callback_data: str) -> Optional[CallbackGuardRule]:
        for rule in self.rules:
            if callback_data.startswith(rule.prefixes):
                return rule
        return None

    async def _answer_blocked(self, callback: CallbackQuery, data: Dict[str, Any]) -> None:
        i18n_data = data.get("i18n_data") or {}
        current_lang = i18n_data.get("current_language", self.settings.DEFAULT_LANGUAGE)
        i18n_instance: JsonI18n = i18n_data.get("i18n_instance", self.i18n_instance)
        message_text = (
            i18n_instance.gettext(current_lang, "error_try_again")
            if i18n_instance
            else "Please try again."
        )
        try:
            await callback.answer(message_text, show_alert=False)
        except Exception as exc:
            logging.debug("CallbackGuardMiddleware: failed to answer blocked callback: %s", exc)
