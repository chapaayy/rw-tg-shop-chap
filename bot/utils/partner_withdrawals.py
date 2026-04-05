import json
from html import escape
from typing import Any, Dict, Optional

from aiogram import types
from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardButton

from db.dal import partner_dal


ADMIN_WITHDRAW_CALLBACK_PREFIX = "partner_withdraw_admin"


def parse_payout_details(raw_value: str) -> Dict[str, Any]:
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        return {}
    return {}


def get_method_label(_: callable, payout_method: str) -> str:
    method = (payout_method or "").strip().lower()
    if method == partner_dal.PARTNER_WITHDRAWAL_METHOD_SBP:
        return _("partners_withdraw_method_sbp")
    if method == partner_dal.PARTNER_WITHDRAWAL_METHOD_USDT_TRC20:
        return _("partners_withdraw_method_usdt_trc20")
    return method or "-"


def get_status_label(_: callable, status: str) -> str:
    status_value = (status or "").strip().lower()
    mapping = {
        partner_dal.PARTNER_WITHDRAWAL_STATUS_PENDING: "partner_withdraw_admin_status_pending",
        partner_dal.PARTNER_WITHDRAWAL_STATUS_APPROVED: "partner_withdraw_admin_status_approved",
        partner_dal.PARTNER_WITHDRAWAL_STATUS_REJECTED: "partner_withdraw_admin_status_rejected",
        partner_dal.PARTNER_WITHDRAWAL_STATUS_PAID: "partner_withdraw_admin_status_paid",
        partner_dal.PARTNER_WITHDRAWAL_STATUS_CANCELLED: "partner_withdraw_admin_status_cancelled",
    }
    key = mapping.get(status_value)
    if not key:
        return status_value or "-"
    return _(key)


def format_payout_details_html(_: callable, payout_method: str, details: Dict[str, Any]) -> str:
    method = (payout_method or "").strip().lower()
    if method == partner_dal.PARTNER_WITHDRAWAL_METHOD_SBP:
        phone = escape(str(details.get("phone") or "-"))
        bank = escape(str(details.get("bank") or "-"))
        fio = escape(str(details.get("fio") or "-"))
        return (
            f"{_('partners_withdraw_field_phone')}: <code>{phone}</code>\n"
            f"{_('partners_withdraw_field_bank')}: <code>{bank}</code>\n"
            f"{_('partners_withdraw_field_fio')}: <code>{fio}</code>"
        )

    wallet = escape(str(details.get("wallet_address") or "-"))
    return f"{_('partners_withdraw_field_wallet')}: <code>{wallet}</code>"


def build_admin_request_keyboard(
    _,
    *,
    request_id: int,
    status: str,
) -> Optional[types.InlineKeyboardMarkup]:
    status_value = (status or "").strip().lower()
    builder = InlineKeyboardBuilder()

    if status_value == partner_dal.PARTNER_WITHDRAWAL_STATUS_PENDING:
        builder.row(
            InlineKeyboardButton(
                text=_("partner_withdraw_admin_accept_button"),
                callback_data=f"{ADMIN_WITHDRAW_CALLBACK_PREFIX}:approve:{request_id}",
            ),
            InlineKeyboardButton(
                text=_("partner_withdraw_admin_reject_button"),
                callback_data=f"{ADMIN_WITHDRAW_CALLBACK_PREFIX}:reject:{request_id}",
            ),
        )
        return builder.as_markup()

    if status_value == partner_dal.PARTNER_WITHDRAWAL_STATUS_APPROVED:
        builder.row(
            InlineKeyboardButton(
                text=_("partner_withdraw_admin_pay_button"),
                callback_data=f"{ADMIN_WITHDRAW_CALLBACK_PREFIX}:pay:{request_id}",
            )
        )
        return builder.as_markup()

    return None
