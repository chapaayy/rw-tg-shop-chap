import logging
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Bot
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError

from config.settings import Settings
from db.dal import partner_dal, user_dal
from db.models import PartnerAccount, PartnerWithdrawalRequest
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

    async def build_partner_link(
        self, bot_username: str, slug: str, *, use_partner_prefix: bool
    ) -> str:
        payload = f"p_{slug}" if use_partner_prefix else slug
        return f"https://t.me/{bot_username}?start={payload}"

    def get_withdrawal_hold_days(self) -> int:
        return max(0, int(self.settings.REFERRAL_WITHDRAWAL_HOLD_DAYS or 0))

    def get_withdrawal_min_amount(self) -> float:
        return max(0.0, float(self.settings.REFERRAL_WITHDRAWAL_MIN_AMOUNT_RUB or 0.0))

    def is_withdrawals_enabled(self) -> bool:
        return bool(self.settings.REFERRAL_WITHDRAWALS_ENABLED)

    async def get_partner_withdrawal_stats(
        self,
        session: AsyncSession,
        *,
        user_id: int,
    ) -> Dict[str, float]:
        money = await partner_dal.get_partner_money_stats(session, user_id)
        total_income = float(money["income"])

        hold_days = self.get_withdrawal_hold_days()
        hold_cutoff = datetime.now(timezone.utc) - timedelta(days=hold_days)
        hold_eligible_income = await partner_dal.get_partner_income_until_datetime(
            session,
            user_id,
            max_created_at=hold_cutoff,
        )

        withdrawal_aggregates = await partner_dal.get_partner_withdrawal_aggregates(
            session, user_id
        )
        reserved = float(withdrawal_aggregates["reserved"])
        in_process = float(withdrawal_aggregates["in_process"])
        paid = float(withdrawal_aggregates["paid"])

        available = round(max(0.0, hold_eligible_income - reserved - paid), 2)

        return {
            "hold_days": float(hold_days),
            "total_income": round(total_income, 2),
            "hold_eligible_income": round(hold_eligible_income, 2),
            "reserved": round(reserved, 2),
            "in_process": round(in_process, 2),
            "paid": round(paid, 2),
            "available_to_withdraw": available,
        }

    async def get_active_withdrawal_request(
        self, session: AsyncSession, *, user_id: int
    ) -> Optional[PartnerWithdrawalRequest]:
        return await partner_dal.get_active_partner_withdrawal_request_for_user(
            session, user_id
        )

    async def create_partner_withdrawal_request(
        self,
        session: AsyncSession,
        *,
        user_id: int,
        amount: float,
        payout_method: str,
        payout_details: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not self.is_withdrawals_enabled():
            return {"created": False, "reason": "withdrawals_disabled"}

        normalized_method = (payout_method or "").strip().lower()
        if normalized_method not in {
            partner_dal.PARTNER_WITHDRAWAL_METHOD_SBP,
            partner_dal.PARTNER_WITHDRAWAL_METHOD_USDT_TRC20,
        }:
            return {"created": False, "reason": "invalid_method"}

        amount_value = round(float(amount or 0.0), 2)
        if amount_value <= 0:
            return {"created": False, "reason": "invalid_amount"}

        min_amount = self.get_withdrawal_min_amount()
        if amount_value < min_amount:
            return {
                "created": False,
                "reason": "below_min_amount",
                "min_amount": min_amount,
            }

        active = await self.get_active_withdrawal_request(session, user_id=user_id)
        if active:
            return {
                "created": False,
                "reason": "active_request_exists",
                "active_request": active,
            }

        stats = await self.get_partner_withdrawal_stats(session, user_id=user_id)
        available = float(stats["available_to_withdraw"])
        if amount_value > available:
            return {
                "created": False,
                "reason": "insufficient_available_balance",
                "available_to_withdraw": available,
            }

        payload_details = json.dumps(payout_details, ensure_ascii=False)

        try:
            model = await partner_dal.create_partner_withdrawal_request(
                session,
                user_id=user_id,
                amount=amount_value,
                payout_method=normalized_method,
                payout_details=payload_details,
                available_balance_snapshot=available,
                in_process_balance_snapshot=float(stats["in_process"]),
                total_income_snapshot=float(stats["total_income"]),
            )
        except IntegrityError:
            logging.info(
                "Partner withdrawal request creation hit active-request guard for user %s",
                user_id,
            )
            return {"created": False, "reason": "active_request_exists"}

        return {
            "created": True,
            "reason": "ok",
            "request": model,
            "stats_snapshot": stats,
        }

    async def attach_admin_message_to_withdrawal_request(
        self,
        session: AsyncSession,
        *,
        request_id: int,
        admin_chat_id: int,
        admin_thread_id: Optional[int],
        admin_message_id: int,
    ) -> Optional[PartnerWithdrawalRequest]:
        return await partner_dal.set_partner_withdrawal_admin_message_meta(
            session,
            request_id=request_id,
            admin_chat_id=admin_chat_id,
            admin_thread_id=admin_thread_id,
            admin_message_id=admin_message_id,
        )

    async def get_partner_withdrawal_request(
        self, session: AsyncSession, request_id: int
    ) -> Optional[PartnerWithdrawalRequest]:
        return await partner_dal.get_partner_withdrawal_request_by_id(session, request_id)

    async def approve_withdrawal_request(
        self, session: AsyncSession, *, request_id: int, admin_id: int
    ) -> Optional[PartnerWithdrawalRequest]:
        return await partner_dal.approve_partner_withdrawal_request(
            session,
            request_id=request_id,
            admin_id=admin_id,
        )

    async def reject_withdrawal_request(
        self,
        session: AsyncSession,
        *,
        request_id: int,
        admin_id: int,
        reject_reason: Optional[str] = None,
    ) -> Optional[PartnerWithdrawalRequest]:
        return await partner_dal.reject_partner_withdrawal_request(
            session,
            request_id=request_id,
            admin_id=admin_id,
            reject_reason=reject_reason,
        )

    async def pay_withdrawal_request(
        self, session: AsyncSession, *, request_id: int, admin_id: int
    ) -> Optional[PartnerWithdrawalRequest]:
        return await partner_dal.mark_partner_withdrawal_request_paid(
            session,
            request_id=request_id,
            admin_id=admin_id,
        )

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
        withdrawal_stats = await self.get_partner_withdrawal_stats(
            session, user_id=user_id
        )
        referrals = await partner_dal.get_partner_referrals_with_income(
            session, user_id, limit=referrals_limit
        )
        effective_percent = await self.get_effective_percent(session, user_id)

        default_link = await self.build_partner_link(
            bot_username, account.default_slug, use_partner_prefix=True
        )
        custom_link = (
            await self.build_partner_link(
                bot_username, account.custom_slug, use_partner_prefix=False
            )
            if account.custom_slug
            else None
        )
        active_link = custom_link or default_link

        return {
            "account": account,
            "active_link": active_link,
            "effective_percent": effective_percent,
            "invited_count": counts["invited_count"],
            "paid_count": counts["paid_count"],
            "turnover": money["turnover"],
            "income": money["income"],
            "available_to_withdraw": withdrawal_stats["available_to_withdraw"],
            "withdrawal_in_process": withdrawal_stats["in_process"],
            "referrals": referrals,
        }

    async def get_user_referrals_page(
        self,
        session: AsyncSession,
        *,
        user_id: int,
        page: int,
        page_size: int = 10,
    ) -> Dict[str, Any]:
        safe_page = max(0, page)
        safe_page_size = max(1, page_size)
        total = await partner_dal.get_partner_referrals_total_count(session, user_id)
        total_pages = max(1, (total + safe_page_size - 1) // safe_page_size)
        if safe_page >= total_pages:
            safe_page = total_pages - 1
        offset = safe_page * safe_page_size
        rows = await partner_dal.get_partner_referrals_with_income(
            session,
            user_id,
            limit=safe_page_size,
            offset=offset,
        )
        return {
            "rows": rows,
            "total": total,
            "current_page": safe_page,
            "total_pages": total_pages,
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
        if normalized.startswith("promo_"):
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
