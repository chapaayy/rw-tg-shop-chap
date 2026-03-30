import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Bot
from sqlalchemy.ext.asyncio import AsyncSession

from config.settings import Settings
from db.dal import partner_dal, user_dal
from db.models import PartnerAccount
from bot.middlewares.i18n import JsonI18n


SLUG_PATTERN = re.compile(r"^[a-z0-9_-]{4,32}$")
RESERVED_SLUGS = {
    "admin",
    "ads",
    "api",
    "back",
    "help",
    "language",
    "main",
    "partner",
    "partners",
    "payment",
    "payments",
    "promo",
    "ref",
    "start",
    "stats",
    "support",
    "system",
    "trial",
    "webhook",
}


class PartnerService:
    def __init__(self, settings: Settings, bot: Bot, i18n: JsonI18n):
        self.settings = settings
        self.bot = bot
        self.i18n = i18n

    @staticmethod
    def normalize_slug(value: str) -> str:
        return (value or "").strip().lower()

    @staticmethod
    def get_active_slug(account: PartnerAccount) -> str:
        return account.custom_slug or account.default_slug

    async def ensure_partner_account(self, session: AsyncSession, user_id: int) -> PartnerAccount:
        return await partner_dal.ensure_partner_account(session, user_id)

    async def get_program_settings(self, session: AsyncSession):
        return await partner_dal.get_or_create_program_settings(session)

    async def get_effective_percent(self, session: AsyncSession, user_id: int) -> float:
        settings_model = await self.get_program_settings(session)
        account = await self.ensure_partner_account(session, user_id)
        if account.personal_percent is not None:
            return float(account.personal_percent)
        return float(settings_model.default_percent)

    async def build_partner_link(self, bot_username: str, slug: str) -> str:
        return f"https://t.me/{bot_username}?start=p_{slug}"

    async def get_user_partner_dashboard(
        self,
        session: AsyncSession,
        *,
        user_id: int,
        bot_username: str,
        referrals_limit: int = 25,
    ) -> Dict[str, Any]:
        account = await self.ensure_partner_account(session, user_id)
        counts = await partner_dal.get_partner_referral_counts(session, user_id)
        money = await partner_dal.get_partner_money_stats(session, user_id)
        referrals = await partner_dal.get_partner_referrals_with_income(
            session, user_id, limit=referrals_limit
        )
        effective_percent = await self.get_effective_percent(session, user_id)

        default_link = await self.build_partner_link(bot_username, account.default_slug)
        custom_link = (
            await self.build_partner_link(bot_username, account.custom_slug)
            if account.custom_slug
            else None
        )
        active_link = await self.build_partner_link(bot_username, self.get_active_slug(account))

        return {
            "account": account,
            "default_link": default_link,
            "custom_link": custom_link,
            "active_link": active_link,
            "effective_percent": effective_percent,
            "invited_count": counts["invited_count"],
            "paid_count": counts["paid_count"],
            "turnover": money["turnover"],
            "income": money["income"],
            "referrals": referrals,
        }

    async def validate_custom_slug(
        self,
        session: AsyncSession,
        *,
        user_id: int,
        slug: str,
    ) -> Tuple[bool, str]:
        normalized = self.normalize_slug(slug)
        if not SLUG_PATTERN.match(normalized):
            return False, "partners_invalid_slug_format"
        if normalized in RESERVED_SLUGS:
            return False, "partners_slug_reserved"
        if normalized.startswith("p_"):
            return False, "partners_slug_reserved"
        if normalized == partner_dal.build_default_slug(user_id):
            return False, "partners_slug_is_default"
        available = await partner_dal.is_slug_available(
            session, normalized, exclude_user_id=user_id
        )
        if not available:
            return False, "partners_slug_taken"
        return True, normalized

    async def set_custom_slug(
        self, session: AsyncSession, *, user_id: int, slug: str
    ) -> PartnerAccount:
        normalized = self.normalize_slug(slug)
        return await partner_dal.set_custom_slug(session, user_id, normalized)

    async def clear_custom_slug(self, session: AsyncSession, *, user_id: int) -> PartnerAccount:
        return await partner_dal.clear_custom_slug(session, user_id)

    async def bind_referred_user_by_slug(
        self,
        session: AsyncSession,
        *,
        invited_user_id: int,
        slug: str,
    ) -> Dict[str, Any]:
        normalized = self.normalize_slug(slug)
        if not normalized:
            return {"bound": False, "reason": "empty"}

        settings_model = await self.get_program_settings(session)
        if not settings_model.is_enabled:
            return {"bound": False, "reason": "program_disabled"}

        account = await partner_dal.get_partner_by_slug(session, normalized)
        if not account:
            return {"bound": False, "reason": "not_found"}
        if account.user_id == invited_user_id:
            return {"bound": False, "reason": "self_referral"}
        if not account.is_enabled:
            return {"bound": False, "reason": "partner_disabled"}

        await self.ensure_partner_account(session, invited_user_id)
        created, referral = await partner_dal.bind_referral_once(
            session,
            partner_user_id=account.user_id,
            invited_user_id=invited_user_id,
            linked_slug=normalized,
        )
        return {
            "bound": bool(created),
            "partner_user_id": account.user_id,
            "reason": "already_bound" if not created else "ok",
            "referral": referral,
        }

    async def apply_partner_commission_for_payment(
        self,
        session: AsyncSession,
        *,
        payment_id: int,
        invited_user_id: int,
        payment_amount: float,
        sale_mode: str,
        currency: str = "RUB",
    ) -> Dict[str, Any]:
        settings_model = await self.get_program_settings(session)
        if not settings_model.is_enabled:
            return {"applied": False, "reason": "program_disabled"}

        referral = await partner_dal.get_referral_by_invited_user(session, invited_user_id)
        if not referral:
            return {"applied": False, "reason": "referral_not_found"}

        account = await self.ensure_partner_account(session, referral.partner_user_id)
        if not account.is_enabled:
            return {"applied": False, "reason": "partner_disabled"}

        if sale_mode == "traffic" and not settings_model.allow_traffic_commission:
            return {"applied": False, "reason": "traffic_disabled"}

        amount_value = float(payment_amount)
        if amount_value < float(settings_model.min_payment_amount):
            return {"applied": False, "reason": "below_min_amount"}

        percent = (
            float(account.personal_percent)
            if account.personal_percent is not None
            else float(settings_model.default_percent)
        )
        if percent <= 0:
            return {"applied": False, "reason": "percent_zero"}

        commission_amount = round(amount_value * percent / 100.0, 2)
        if commission_amount <= 0:
            return {"applied": False, "reason": "commission_zero"}

        created, model = await partner_dal.create_commission_if_absent(
            session,
            partner_user_id=referral.partner_user_id,
            invited_user_id=invited_user_id,
            payment_id=payment_id,
            payment_amount=amount_value,
            percent_applied=percent,
            commission_amount=commission_amount,
            currency=currency,
            sale_mode=sale_mode,
        )
        return {
            "applied": bool(created),
            "reason": "already_exists" if not created else "ok",
            "model": model,
            "partner_user_id": referral.partner_user_id,
            "percent": percent,
            "commission_amount": commission_amount,
        }

    async def get_partner_admin_card_data(
        self, session: AsyncSession, partner_user_id: int
    ) -> Optional[Dict[str, Any]]:
        user = await user_dal.get_user_by_id(session, partner_user_id)
        if not user:
            return None

        account = await self.ensure_partner_account(session, partner_user_id)
        counts = await partner_dal.get_partner_referral_counts(session, partner_user_id)
        money = await partner_dal.get_partner_money_stats(session, partner_user_id)
        settings_model = await self.get_program_settings(session)
        effective_percent = (
            float(account.personal_percent)
            if account.personal_percent is not None
            else float(settings_model.default_percent)
        )
        referrals = await partner_dal.get_partner_referrals_with_income(
            session, partner_user_id, limit=20
        )
        commissions = await partner_dal.get_partner_commission_history(
            session, partner_user_id, limit=20
        )

        return {
            "user": user,
            "account": account,
            "invited_count": counts["invited_count"],
            "paid_count": counts["paid_count"],
            "turnover": money["turnover"],
            "income": money["income"],
            "effective_percent": effective_percent,
            "referrals": referrals,
            "commissions": commissions,
            "program_settings": settings_model,
        }
