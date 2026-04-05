import json
import logging
from html import escape
from typing import Any, Dict, Optional

from aiogram import F, Router, types
from sqlalchemy.ext.asyncio import AsyncSession

from bot.middlewares.i18n import JsonI18n
from bot.services.partner_service import PartnerService
from bot.utils.partner_withdrawals import (
    ADMIN_WITHDRAW_CALLBACK_PREFIX,
    build_admin_request_keyboard,
    format_payout_details_html,
    get_method_label,
    get_status_label,
)
from bot.utils.text_sanitizer import sanitize_display_name, sanitize_username
from config.settings import Settings
from db.dal import user_dal

router = Router(name="admin_partner_withdrawals_router")


def _format_money(value: Any) -> str:
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "0.00"


def _build_admin_withdrawal_text(
    _,
    *,
    request_model,
    db_user,
) -> str:
    try:
        details_dict = json.loads(request_model.payout_details or "{}")
        if not isinstance(details_dict, dict):
            details_dict = {}
    except Exception:
        details_dict = {}

    details_text = format_payout_details_html(_, request_model.payout_method, details_dict)

    safe_first_name = sanitize_display_name(db_user.first_name) if db_user else None
    if not safe_first_name:
        safe_first_name = f"ID {request_model.user_id}"
    safe_username = sanitize_username(db_user.username) if db_user and db_user.username else None

    status_text = get_status_label(_, request_model.status)
    method_text = get_method_label(_, request_model.payout_method)

    created_at = request_model.created_at
    created_text = created_at.strftime("%Y-%m-%d %H:%M:%S") if created_at else "-"

    lines = [
        _("partner_withdraw_admin_new_request_title"),
        "",
        _("partner_withdraw_admin_request_id_label", request_id=request_model.request_id),
        _("partner_withdraw_admin_user_label", user=escape(safe_first_name)),
        _("partner_withdraw_admin_username_label", username=f"@{safe_username}" if safe_username else "-"),
        _("partner_withdraw_admin_tg_id_label", user_id=request_model.user_id),
        "",
        _("partner_withdraw_admin_amount_label", amount=_format_money(request_model.amount)),
        _("partner_withdraw_admin_method_label", method=method_text),
        _("partner_withdraw_admin_details_label"),
        details_text,
        "",
        _("partner_withdraw_admin_total_income_label", total_income=_format_money(request_model.total_income_snapshot)),
        _(
            "partner_withdraw_admin_available_snapshot_label",
            available=_format_money(request_model.available_balance_snapshot),
        ),
        _(
            "partner_withdraw_admin_in_process_snapshot_label",
            in_process=_format_money(request_model.in_process_balance_snapshot),
        ),
        _("partner_withdraw_admin_created_at_label", created_at=created_text),
        _("partner_withdraw_admin_status_label", status=status_text),
    ]

    if request_model.processed_at:
        lines.append(
            _("partner_withdraw_admin_processed_at_label", processed_at=request_model.processed_at.strftime("%Y-%m-%d %H:%M:%S"))
        )
    if request_model.paid_at:
        lines.append(
            _("partner_withdraw_admin_paid_at_label", paid_at=request_model.paid_at.strftime("%Y-%m-%d %H:%M:%S"))
        )
    if request_model.reject_reason:
        lines.append(
            _("partner_withdraw_admin_reject_reason_label", reject_reason=escape(request_model.reject_reason))
        )
    return "\n".join(lines)


def _extract_request_id(callback_data: Optional[str], expected_action: str) -> int:
    parts = (callback_data or "").split(":")
    if len(parts) != 3:
        raise ValueError("Invalid callback payload")
    prefix, action, raw_request_id = parts
    if prefix != ADMIN_WITHDRAW_CALLBACK_PREFIX or action != expected_action:
        raise ValueError("Invalid callback payload")
    return int(raw_request_id)


def _is_expected_chat_context(callback: types.CallbackQuery, settings: Settings) -> bool:
    if not callback.message:
        return False

    target_chat_id = settings.REFERRAL_WITHDRAWAL_REQUESTS_CHAT_ID
    target_thread_id = settings.REFERRAL_WITHDRAWAL_REQUESTS_THREAD_ID
    if target_chat_id is None:
        return False

    if callback.message.chat.id != target_chat_id:
        return False

    if target_thread_id is not None and callback.message.message_thread_id != target_thread_id:
        return False

    return True


async def _notify_user_status_change(
    *,
    request_model,
    status: str,
    settings: Settings,
    i18n: JsonI18n,
    session: AsyncSession,
    callback: types.CallbackQuery,
) -> None:
    user = await user_dal.get_user_by_id(session, request_model.user_id)
    lang = user.language_code if user and user.language_code else settings.DEFAULT_LANGUAGE
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    if status == "approved":
        text = _(
            "partners_withdraw_notify_approved",
            request_id=request_model.request_id,
            amount=_format_money(request_model.amount),
        )
    elif status == "rejected":
        text = _(
            "partners_withdraw_notify_rejected",
            request_id=request_model.request_id,
            amount=_format_money(request_model.amount),
        )
    elif status == "paid":
        text = _(
            "partners_withdraw_notify_paid",
            request_id=request_model.request_id,
            amount=_format_money(request_model.amount),
        )
    else:
        return

    try:
        await callback.bot.send_message(
            chat_id=request_model.user_id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as exc:
        logging.error(
            "Failed to notify user %s about partner withdrawal request %s status %s: %s",
            request_model.user_id,
            request_model.request_id,
            status,
            exc,
        )


async def _render_admin_request_message(
    *,
    callback: types.CallbackQuery,
    request_model,
    i18n: JsonI18n,
    settings: Settings,
    session: AsyncSession,
) -> None:
    if not callback.message:
        return

    admin_lang = settings.DEFAULT_LANGUAGE
    _admin = lambda key, **kwargs: i18n.gettext(admin_lang, key, **kwargs)
    db_user = await user_dal.get_user_by_id(session, request_model.user_id)
    text = _build_admin_withdrawal_text(_admin, request_model=request_model, db_user=db_user)
    markup = build_admin_request_keyboard(
        _admin,
        request_id=request_model.request_id,
        status=request_model.status,
    )

    await callback.message.edit_text(
        text=text,
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=markup,
    )


@router.callback_query(F.data.startswith(f"{ADMIN_WITHDRAW_CALLBACK_PREFIX}:approve:"))
async def approve_withdrawal_request_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
    partner_service: PartnerService,
):
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    if not _is_expected_chat_context(callback, settings):
        await callback.answer(_("partner_withdraw_admin_wrong_context"), show_alert=True)
        return

    try:
        request_id = _extract_request_id(callback.data, "approve")
    except (ValueError, TypeError):
        await callback.answer(_("partner_withdraw_admin_request_not_found"), show_alert=True)
        return

    updated = await partner_service.approve_withdrawal_request(
        session,
        request_id=request_id,
        admin_id=callback.from_user.id,
    )
    request_model = updated or await partner_service.get_partner_withdrawal_request(session, request_id)
    if not request_model:
        await callback.answer(_("partner_withdraw_admin_request_not_found"), show_alert=True)
        return

    await _render_admin_request_message(
        callback=callback,
        request_model=request_model,
        i18n=i18n,
        settings=settings,
        session=session,
    )

    if not updated:
        await callback.answer(_("partner_withdraw_admin_already_processed"), show_alert=True)
        return

    await _notify_user_status_change(
        request_model=request_model,
        status="approved",
        settings=settings,
        i18n=i18n,
        session=session,
        callback=callback,
    )
    await callback.answer(_("partner_withdraw_admin_approved_alert"), show_alert=False)


@router.callback_query(F.data.startswith(f"{ADMIN_WITHDRAW_CALLBACK_PREFIX}:reject:"))
async def reject_withdrawal_request_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
    partner_service: PartnerService,
):
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    if not _is_expected_chat_context(callback, settings):
        await callback.answer(_("partner_withdraw_admin_wrong_context"), show_alert=True)
        return

    try:
        request_id = _extract_request_id(callback.data, "reject")
    except (ValueError, TypeError):
        await callback.answer(_("partner_withdraw_admin_request_not_found"), show_alert=True)
        return

    updated = await partner_service.reject_withdrawal_request(
        session,
        request_id=request_id,
        admin_id=callback.from_user.id,
        reject_reason="Rejected by admin",
    )
    request_model = updated or await partner_service.get_partner_withdrawal_request(session, request_id)
    if not request_model:
        await callback.answer(_("partner_withdraw_admin_request_not_found"), show_alert=True)
        return

    await _render_admin_request_message(
        callback=callback,
        request_model=request_model,
        i18n=i18n,
        settings=settings,
        session=session,
    )

    if not updated:
        await callback.answer(_("partner_withdraw_admin_already_processed"), show_alert=True)
        return

    await _notify_user_status_change(
        request_model=request_model,
        status="rejected",
        settings=settings,
        i18n=i18n,
        session=session,
        callback=callback,
    )
    await callback.answer(_("partner_withdraw_admin_rejected_alert"), show_alert=False)


@router.callback_query(F.data.startswith(f"{ADMIN_WITHDRAW_CALLBACK_PREFIX}:pay:"))
async def pay_withdrawal_request_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
    partner_service: PartnerService,
):
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    if not _is_expected_chat_context(callback, settings):
        await callback.answer(_("partner_withdraw_admin_wrong_context"), show_alert=True)
        return

    try:
        request_id = _extract_request_id(callback.data, "pay")
    except (ValueError, TypeError):
        await callback.answer(_("partner_withdraw_admin_request_not_found"), show_alert=True)
        return

    updated = await partner_service.pay_withdrawal_request(
        session,
        request_id=request_id,
        admin_id=callback.from_user.id,
    )
    request_model = updated or await partner_service.get_partner_withdrawal_request(session, request_id)
    if not request_model:
        await callback.answer(_("partner_withdraw_admin_request_not_found"), show_alert=True)
        return

    await _render_admin_request_message(
        callback=callback,
        request_model=request_model,
        i18n=i18n,
        settings=settings,
        session=session,
    )

    if not updated:
        await callback.answer(_("partner_withdraw_admin_already_processed"), show_alert=True)
        return

    await _notify_user_status_change(
        request_model=request_model,
        status="paid",
        settings=settings,
        i18n=i18n,
        session=session,
        callback=callback,
    )
    await callback.answer(_("partner_withdraw_admin_paid_alert"), show_alert=False)
