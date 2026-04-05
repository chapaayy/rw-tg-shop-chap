import json
import logging
import re
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html import escape
from typing import Any, Dict, Optional, Union

from aiogram import F, Router, types, Bot
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardButton
from sqlalchemy.ext.asyncio import AsyncSession

from bot.middlewares.i18n import JsonI18n
from bot.services.partner_service import PartnerService
from bot.states.user_states import UserPartnerStates, UserPartnerWithdrawalStates
from bot.utils.partner_withdrawals import (
    build_admin_request_keyboard,
    format_payout_details_html,
    get_method_label,
    get_status_label,
)
from bot.utils.text_sanitizer import sanitize_display_name, sanitize_username
from bot.keyboards.inline.user_keyboards import (
    get_back_to_main_menu_markup,
    get_partner_menu_keyboard,
)
from config.settings import Settings
from db.dal import partner_dal

router = Router(name="user_partners_router")
REFERRALS_PAGE_SIZE = 10
WITHDRAW_AMOUNT_PATTERN = re.compile(r"^\d+([.,]\d{1,2})?$")
SBP_PHONE_PATTERN = re.compile(r"^\+?\d{10,15}$")
TRC20_WALLET_PATTERN = re.compile(r"^T[1-9A-HJ-NP-Za-km-z]{33}$")


def _format_referral_name(row: dict, fallback_id: int) -> str:
    first_name = sanitize_display_name(row.get("first_name")) if row.get("first_name") else None
    username = sanitize_username(row.get("username")) if row.get("username") else None
    if first_name and username:
        return f"{first_name} (@{username})"
    if first_name:
        return first_name
    if username:
        return f"@{username}"
    return f"ID {fallback_id}"


def _format_money(value: Any) -> str:
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "0.00"


def _parse_amount_input(raw_value: str) -> Optional[float]:
    normalized = (raw_value or "").strip().replace(" ", "").replace(",", ".")
    if not normalized or not WITHDRAW_AMOUNT_PATTERN.match(normalized):
        return None
    try:
        value = Decimal(normalized).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError):
        return None
    if value <= 0:
        return None
    return float(value)


def _normalize_sbp_phone(raw_value: str) -> Optional[str]:
    compact = re.sub(r"[^\d+]", "", raw_value or "")
    if not compact:
        return None

    plus_sign = "+" if compact.startswith("+") else ""
    digits = compact[1:] if plus_sign else compact

    if digits.startswith("8") and len(digits) == 11:
        digits = f"7{digits[1:]}"
        plus_sign = "+"
    elif digits.startswith("7") and len(digits) == 11:
        plus_sign = "+"

    normalized = f"{plus_sign}{digits}" if plus_sign else digits
    if not SBP_PHONE_PATTERN.match(normalized):
        return None
    return normalized


def _compose_dashboard_text(_: callable, dashboard: dict) -> str:
    return _(
        "partners_dashboard_header",
        active_link=dashboard["active_link"],
        percent=f"{dashboard['effective_percent']:.2f}",
        invited_count=dashboard["invited_count"],
        paid_count=dashboard["paid_count"],
        turnover=f"{dashboard['turnover']:.2f}",
        income=f"{dashboard['income']:.2f}",
        available_to_withdraw=f"{dashboard['available_to_withdraw']:.2f}",
        withdrawal_in_process=f"{dashboard['withdrawal_in_process']:.2f}",
    )


def _build_referrals_keyboard(
    _,
    *,
    current_page: int,
    total_pages: int,
) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    nav_buttons = []
    if current_page > 0:
        nav_buttons.append(
            InlineKeyboardButton(
                text=_("partners_prev_page_button"),
                callback_data=f"partners_action:referrals:{current_page - 1}",
            )
        )
    nav_buttons.append(
        InlineKeyboardButton(
            text=f"{current_page + 1}/{total_pages}",
            callback_data="partners_action:referrals_noop",
        )
    )
    if current_page < total_pages - 1:
        nav_buttons.append(
            InlineKeyboardButton(
                text=_("partners_next_page_button"),
                callback_data=f"partners_action:referrals:{current_page + 1}",
            )
        )
    builder.row(*nav_buttons)
    builder.row(
        InlineKeyboardButton(
            text=_("partners_back_button"),
            callback_data="partners_action:open",
        )
    )
    return builder.as_markup()


def _build_withdraw_methods_keyboard(_) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_("partners_withdraw_method_sbp"),
            callback_data=f"partners_action:withdraw_method:{partner_dal.PARTNER_WITHDRAWAL_METHOD_SBP}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=_("partners_withdraw_method_usdt_trc20"),
            callback_data=f"partners_action:withdraw_method:{partner_dal.PARTNER_WITHDRAWAL_METHOD_USDT_TRC20}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=_("partners_withdraw_cancel_button"),
            callback_data="partners_action:withdraw_cancel",
        )
    )
    return builder.as_markup()


def _build_withdraw_cancel_keyboard(_) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_("partners_withdraw_cancel_button"),
            callback_data="partners_action:withdraw_cancel",
        )
    )
    return builder.as_markup()


def _build_withdraw_back_keyboard(_) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_("partners_back_button"),
            callback_data="partners_action:open",
        )
    )
    return builder.as_markup()


def _build_withdraw_amount_keyboard(_) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_("partners_withdraw_all_amount_button"),
            callback_data="partners_action:withdraw_all",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=_("partners_withdraw_cancel_button"),
            callback_data="partners_action:withdraw_cancel",
        )
    )
    return builder.as_markup()


def _build_withdraw_confirmation_keyboard(_) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_("partners_withdraw_confirm_button"),
            callback_data="partners_action:withdraw_confirm",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=_("partners_withdraw_cancel_button"),
            callback_data="partners_action:withdraw_cancel",
        )
    )
    return builder.as_markup()


def _compose_withdrawal_summary_text(_: callable, stats: Dict[str, float]) -> str:
    return _(
        "partners_withdraw_start_header",
        total_income=_format_money(stats.get("total_income")),
        available=_format_money(stats.get("available_to_withdraw")),
        in_process=_format_money(stats.get("in_process")),
    )


def _build_admin_withdrawal_text(
    _,
    *,
    request_model,
    tg_user: types.User,
) -> str:
    try:
        details_dict = json.loads(request_model.payout_details or "{}")
        if not isinstance(details_dict, dict):
            details_dict = {}
    except Exception:
        details_dict = {}

    details_text = format_payout_details_html(_, request_model.payout_method, details_dict)

    safe_first_name = sanitize_display_name(tg_user.first_name) or f"ID {tg_user.id}"
    safe_username = sanitize_username(tg_user.username) if tg_user.username else None

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


async def _show_referrals_page(
    callback: types.CallbackQuery,
    *,
    page: int,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
) -> None:
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    program_settings = await partner_service.get_program_settings(session)
    if not program_settings.is_enabled:
        await callback.message.edit_text(
            _("partners_program_disabled"),
            reply_markup=get_back_to_main_menu_markup(current_lang, i18n),
        )
        await callback.answer()
        return

    data = await partner_service.get_user_referrals_page(
        session,
        user_id=callback.from_user.id,
        page=page,
        page_size=REFERRALS_PAGE_SIZE,
    )
    rows = data["rows"]
    if not rows:
        text = _("partners_referrals_list_empty")
    else:
        lines = [
            _(
                "partners_referrals_page_header",
                current_page=data["current_page"] + 1,
                total_pages=data["total_pages"],
            )
        ]
        for row in rows:
            invited_user_id = int(row["invited_user_id"])
            lines.append(
                _(
                    "partners_referral_list_item",
                    user=_format_referral_name(row, invited_user_id),
                    invited_user_id=invited_user_id,
                    payments_count=int(row.get("payments_count") or 0),
                    turnover=f"{float(row.get('turnover') or 0.0):.2f}",
                    income=f"{float(row.get('income') or 0.0):.2f}",
                )
            )
        text = "\n".join(lines)

    markup = _build_referrals_keyboard(
        _,
        current_page=data["current_page"],
        total_pages=data["total_pages"],
    )
    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=markup,
    )
    await callback.answer()


async def show_partner_dashboard(
    event: Union[types.Message, types.CallbackQuery],
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    bot: Bot,
    session: AsyncSession,
) -> None:
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n:
        if isinstance(event, types.CallbackQuery):
            await event.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    target_message = event.message if isinstance(event, types.CallbackQuery) else event
    if not target_message:
        if isinstance(event, types.CallbackQuery):
            await event.answer(_("error_occurred_try_again"), show_alert=True)
        return

    try:
        bot_info = await bot.get_me()
        bot_username = bot_info.username
    except Exception as exc:
        logging.error("Failed to resolve bot username for partner link: %s", exc)
        await target_message.answer(_("partners_error_generating_link"))
        if isinstance(event, types.CallbackQuery):
            await event.answer()
        return

    if not bot_username:
        await target_message.answer(_("partners_error_generating_link"))
        if isinstance(event, types.CallbackQuery):
            await event.answer()
        return

    program_settings = await partner_service.get_program_settings(session)
    if not program_settings.is_enabled:
        disabled_markup = get_back_to_main_menu_markup(current_lang, i18n)
        if isinstance(event, types.CallbackQuery):
            try:
                await target_message.edit_text(
                    _("partners_program_disabled"),
                    reply_markup=disabled_markup,
                )
            except Exception:
                await target_message.answer(
                    _("partners_program_disabled"),
                    reply_markup=disabled_markup,
                )
            await event.answer()
        else:
            await target_message.answer(
                _("partners_program_disabled"),
                reply_markup=disabled_markup,
            )
        return

    dashboard = await partner_service.get_user_partner_dashboard(
        session,
        user_id=event.from_user.id,
        bot_username=bot_username,
        referrals_limit=0,
    )
    text = _compose_dashboard_text(_, dashboard)
    keyboard = get_partner_menu_keyboard(current_lang, i18n)

    if isinstance(event, types.CallbackQuery):
        try:
            await target_message.edit_text(
                text, reply_markup=keyboard, parse_mode="HTML", disable_web_page_preview=True
            )
        except Exception:
            await target_message.answer(
                text, reply_markup=keyboard, parse_mode="HTML", disable_web_page_preview=True
            )
        await event.answer()
    else:
        await target_message.answer(
            text, reply_markup=keyboard, parse_mode="HTML", disable_web_page_preview=True
        )


@router.callback_query(F.data == "partners_action:open")
async def open_partners_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    bot: Bot,
    session: AsyncSession,
):
    await show_partner_dashboard(
        callback, settings, i18n_data, partner_service, bot, session
    )


async def _ask_withdrawal_amount(
    message: types.Message,
    *,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
) -> None:
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n:
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    active_request = await partner_service.get_active_withdrawal_request(
        session, user_id=message.from_user.id
    )
    if active_request:
        await state.clear()
        await message.answer(
            _(
                "partners_withdraw_active_request_exists",
                request_id=active_request.request_id,
                status=get_status_label(_, active_request.status),
            )
        )
        return

    stats = await partner_service.get_partner_withdrawal_stats(
        session, user_id=message.from_user.id
    )
    available = float(stats["available_to_withdraw"])
    if available <= 0:
        await state.clear()
        await message.answer(_("partners_withdraw_no_available"))
        return

    await state.set_state(UserPartnerWithdrawalStates.waiting_for_amount)
    await state.update_data(available_to_withdraw=available)

    text = _(
        "partners_withdraw_enter_amount",
        available=_format_money(available),
    )
    min_amount = partner_service.get_withdrawal_min_amount()
    if min_amount > 0:
        text = f"{text}\n\n{_('partners_withdraw_min_amount_hint', min_amount=_format_money(min_amount))}"

    await message.answer(
        text,
        reply_markup=_build_withdraw_amount_keyboard(_),
    )


async def _show_withdraw_confirmation(
    event: Union[types.Message, types.CallbackQuery],
    *,
    amount: float,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
) -> None:
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n:
        if isinstance(event, types.CallbackQuery):
            await event.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    state_data = await state.get_data()
    payout_method = str(state_data.get("withdraw_method") or "").strip().lower()
    if payout_method not in {
        partner_dal.PARTNER_WITHDRAWAL_METHOD_SBP,
        partner_dal.PARTNER_WITHDRAWAL_METHOD_USDT_TRC20,
    }:
        if isinstance(event, types.CallbackQuery):
            await event.answer(_("error_occurred_try_again"), show_alert=True)
        return

    if payout_method == partner_dal.PARTNER_WITHDRAWAL_METHOD_SBP:
        payout_details = {
            "phone": state_data.get("sbp_phone") or "",
            "bank": state_data.get("sbp_bank") or "",
            "fio": state_data.get("sbp_fio") or "",
        }
    else:
        payout_details = {
            "wallet_address": state_data.get("usdt_wallet_address") or "",
        }

    stats = await partner_service.get_partner_withdrawal_stats(
        session, user_id=event.from_user.id
    )
    available = float(stats["available_to_withdraw"])
    if amount > available:
        if isinstance(event, types.CallbackQuery):
            await event.answer(
                _("partners_withdraw_amount_too_high", available=_format_money(available)),
                show_alert=True,
            )
        else:
            await event.answer(
                _("partners_withdraw_amount_too_high", available=_format_money(available))
            )
        return

    await state.set_state(UserPartnerWithdrawalStates.waiting_for_confirmation)
    await state.update_data(withdraw_amount=round(float(amount), 2))

    method_text = get_method_label(_, payout_method)
    details_text = format_payout_details_html(_, payout_method, payout_details)

    text = _(
        "partners_withdraw_confirm_text",
        amount=_format_money(amount),
        method=method_text,
        available=_format_money(available),
        details=details_text,
    )

    if isinstance(event, types.CallbackQuery):
        if not event.message:
            await event.answer(_("error_occurred_try_again"), show_alert=True)
            return
        await event.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=_build_withdraw_confirmation_keyboard(_),
        )
        await event.answer()
    else:
        await event.answer(
            text,
            parse_mode="HTML",
            reply_markup=_build_withdraw_confirmation_keyboard(_),
        )


@router.callback_query(F.data == "partners_action:withdraw")
async def withdraw_start_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    if not partner_service.is_withdrawals_enabled():
        await callback.answer(_("partners_withdrawals_disabled"), show_alert=True)
        return

    stats = await partner_service.get_partner_withdrawal_stats(
        session, user_id=callback.from_user.id
    )
    overview_text = _compose_withdrawal_summary_text(_, stats)

    active_request = await partner_service.get_active_withdrawal_request(
        session, user_id=callback.from_user.id
    )
    if active_request:
        await state.clear()
        await callback.message.edit_text(
            f"{overview_text}\n\n"
            + _(
                "partners_withdraw_active_request_exists",
                request_id=active_request.request_id,
                status=get_status_label(_, active_request.status),
            ),
            reply_markup=_build_withdraw_back_keyboard(_),
        )
        await callback.answer()
        return

    available = float(stats["available_to_withdraw"])
    if available <= 0:
        await state.clear()
        await callback.message.edit_text(
            f"{overview_text}\n\n{_('partners_withdraw_no_available')}",
            reply_markup=_build_withdraw_back_keyboard(_),
        )
        await callback.answer()
        return

    await state.set_state(UserPartnerWithdrawalStates.waiting_for_method)
    await state.update_data(withdraw_flow_started=True)

    await callback.message.edit_text(
        f"{overview_text}\n\n{_('partners_withdraw_method_prompt')}",
        reply_markup=_build_withdraw_methods_keyboard(_),
    )
    await callback.answer()


@router.callback_query(
    F.data.startswith("partners_action:withdraw_method:"),
    StateFilter(UserPartnerWithdrawalStates.waiting_for_method),
)
async def withdraw_method_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
):
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    payout_method = (callback.data or "").split(":")[-1]
    if payout_method not in {
        partner_dal.PARTNER_WITHDRAWAL_METHOD_SBP,
        partner_dal.PARTNER_WITHDRAWAL_METHOD_USDT_TRC20,
    }:
        await callback.answer(_("error_occurred_try_again"), show_alert=True)
        return

    await state.update_data(withdraw_method=payout_method)

    if payout_method == partner_dal.PARTNER_WITHDRAWAL_METHOD_SBP:
        await state.set_state(UserPartnerWithdrawalStates.waiting_for_sbp_phone)
        await callback.message.edit_text(
            _("partners_withdraw_enter_sbp_phone"),
            reply_markup=_build_withdraw_cancel_keyboard(_),
        )
    else:
        await state.set_state(UserPartnerWithdrawalStates.waiting_for_usdt_wallet)
        await callback.message.edit_text(
            _("partners_withdraw_enter_usdt_wallet"),
            reply_markup=_build_withdraw_cancel_keyboard(_),
        )
    await callback.answer()


@router.message(UserPartnerWithdrawalStates.waiting_for_sbp_phone, F.text)
async def withdraw_sbp_phone_message(
    message: types.Message,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
):
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n:
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    normalized_phone = _normalize_sbp_phone(message.text or "")
    if not normalized_phone:
        await message.answer(_("partners_withdraw_invalid_phone"))
        return

    await state.update_data(sbp_phone=normalized_phone)
    await state.set_state(UserPartnerWithdrawalStates.waiting_for_sbp_bank)
    await message.answer(
        _("partners_withdraw_enter_sbp_bank"),
        reply_markup=_build_withdraw_cancel_keyboard(_),
    )


@router.message(UserPartnerWithdrawalStates.waiting_for_sbp_bank, F.text)
async def withdraw_sbp_bank_message(
    message: types.Message,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
):
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n:
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    bank = (message.text or "").strip()
    if len(bank) < 2 or len(bank) > 100:
        await message.answer(_("partners_withdraw_invalid_bank"))
        return

    await state.update_data(sbp_bank=bank)
    await state.set_state(UserPartnerWithdrawalStates.waiting_for_sbp_fio)
    await message.answer(
        _("partners_withdraw_enter_sbp_fio"),
        reply_markup=_build_withdraw_cancel_keyboard(_),
    )


@router.message(UserPartnerWithdrawalStates.waiting_for_sbp_fio, F.text)
async def withdraw_sbp_fio_message(
    message: types.Message,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n:
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    fio = (message.text or "").strip()
    if len(fio) < 3 or len(fio) > 120:
        await message.answer(_("partners_withdraw_invalid_fio"))
        return

    await state.update_data(sbp_fio=fio)
    await _ask_withdrawal_amount(
        message,
        state=state,
        settings=settings,
        i18n_data=i18n_data,
        partner_service=partner_service,
        session=session,
    )


@router.message(UserPartnerWithdrawalStates.waiting_for_usdt_wallet, F.text)
async def withdraw_usdt_wallet_message(
    message: types.Message,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n:
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    wallet = (message.text or "").strip()
    if not TRC20_WALLET_PATTERN.match(wallet):
        await message.answer(_("partners_withdraw_invalid_usdt_wallet"))
        return

    await state.update_data(usdt_wallet_address=wallet)
    await _ask_withdrawal_amount(
        message,
        state=state,
        settings=settings,
        i18n_data=i18n_data,
        partner_service=partner_service,
        session=session,
    )


@router.message(UserPartnerWithdrawalStates.waiting_for_amount, F.text)
async def withdraw_amount_message(
    message: types.Message,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n:
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    amount = _parse_amount_input(message.text or "")
    if amount is None:
        await message.answer(_("partners_withdraw_amount_invalid"))
        return

    stats = await partner_service.get_partner_withdrawal_stats(
        session, user_id=message.from_user.id
    )
    available = float(stats["available_to_withdraw"])
    if available <= 0:
        await message.answer(_("partners_withdraw_no_available"))
        return

    if amount > available:
        await message.answer(
            _("partners_withdraw_amount_too_high", available=_format_money(available))
        )
        return

    min_amount = partner_service.get_withdrawal_min_amount()
    if min_amount > 0 and amount < min_amount:
        await message.answer(
            _("partners_withdraw_amount_too_low", min_amount=_format_money(min_amount))
        )
        return

    await _show_withdraw_confirmation(
        message,
        amount=amount,
        state=state,
        settings=settings,
        i18n_data=i18n_data,
        partner_service=partner_service,
        session=session,
    )


@router.callback_query(
    F.data == "partners_action:withdraw_all",
    StateFilter(UserPartnerWithdrawalStates.waiting_for_amount),
)
async def withdraw_all_amount_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    stats = await partner_service.get_partner_withdrawal_stats(
        session, user_id=callback.from_user.id
    )
    available = float(stats["available_to_withdraw"])
    if available <= 0:
        await callback.answer(_("partners_withdraw_no_available"), show_alert=True)
        return

    min_amount = partner_service.get_withdrawal_min_amount()
    if min_amount > 0 and available < min_amount:
        await callback.answer(
            _("partners_withdraw_amount_too_low", min_amount=_format_money(min_amount)),
            show_alert=True,
        )
        return

    await _show_withdraw_confirmation(
        callback,
        amount=available,
        state=state,
        settings=settings,
        i18n_data=i18n_data,
        partner_service=partner_service,
        session=session,
    )


@router.callback_query(
    F.data == "partners_action:withdraw_confirm",
    StateFilter(UserPartnerWithdrawalStates.waiting_for_confirmation),
)
async def withdraw_confirm_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
    bot: Bot,
):
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    if not partner_service.is_withdrawals_enabled():
        await callback.answer(_("partners_withdrawals_disabled"), show_alert=True)
        return

    request_chat_id = settings.REFERRAL_WITHDRAWAL_REQUESTS_CHAT_ID
    request_thread_id = settings.REFERRAL_WITHDRAWAL_REQUESTS_THREAD_ID
    if request_chat_id is None:
        await callback.answer(_("partners_withdraw_requests_destination_not_set"), show_alert=True)
        return

    state_data = await state.get_data()
    payout_method = str(state_data.get("withdraw_method") or "").strip().lower()
    amount = float(state_data.get("withdraw_amount") or 0.0)
    if amount <= 0:
        await callback.answer(_("partners_withdraw_amount_invalid"), show_alert=True)
        return

    payout_details: Dict[str, Any]
    if payout_method == partner_dal.PARTNER_WITHDRAWAL_METHOD_SBP:
        payout_details = {
            "phone": str(state_data.get("sbp_phone") or "").strip(),
            "bank": str(state_data.get("sbp_bank") or "").strip(),
            "fio": str(state_data.get("sbp_fio") or "").strip(),
        }
    elif payout_method == partner_dal.PARTNER_WITHDRAWAL_METHOD_USDT_TRC20:
        payout_details = {
            "wallet_address": str(state_data.get("usdt_wallet_address") or "").strip(),
        }
    else:
        await callback.answer(_("error_occurred_try_again"), show_alert=True)
        return

    result = await partner_service.create_partner_withdrawal_request(
        session,
        user_id=callback.from_user.id,
        amount=amount,
        payout_method=payout_method,
        payout_details=payout_details,
    )
    if not result.get("created"):
        reason = result.get("reason")
        if reason == "active_request_exists":
            active_request = result.get("active_request")
            if active_request:
                await callback.answer(
                    _(
                        "partners_withdraw_active_request_exists",
                        request_id=active_request.request_id,
                        status=get_status_label(_, active_request.status),
                    ),
                    show_alert=True,
                )
            else:
                await callback.answer(_("partners_withdraw_active_request_exists_short"), show_alert=True)
            return
        if reason == "below_min_amount":
            await callback.answer(
                _(
                    "partners_withdraw_amount_too_low",
                    min_amount=_format_money(result.get("min_amount") or 0.0),
                ),
                show_alert=True,
            )
            return
        if reason == "insufficient_available_balance":
            await callback.answer(
                _(
                    "partners_withdraw_amount_too_high",
                    available=_format_money(result.get("available_to_withdraw") or 0.0),
                ),
                show_alert=True,
            )
            return

        await callback.answer(_("partners_withdraw_request_create_error"), show_alert=True)
        return

    request_model = result["request"]

    admin_lang = settings.DEFAULT_LANGUAGE
    _admin = lambda key, **kwargs: i18n.gettext(admin_lang, key, **kwargs)
    admin_text = _build_admin_withdrawal_text(
        _admin,
        request_model=request_model,
        tg_user=callback.from_user,
    )
    admin_markup = build_admin_request_keyboard(
        _admin,
        request_id=request_model.request_id,
        status=request_model.status,
    )

    try:
        kwargs: Dict[str, Any] = {
            "chat_id": request_chat_id,
            "text": admin_text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "reply_markup": admin_markup,
        }
        if request_thread_id:
            kwargs["message_thread_id"] = request_thread_id
        admin_message = await bot.send_message(**kwargs)
        await partner_service.attach_admin_message_to_withdrawal_request(
            session,
            request_id=request_model.request_id,
            admin_chat_id=request_chat_id,
            admin_thread_id=request_thread_id,
            admin_message_id=admin_message.message_id,
        )
    except Exception:
        await session.rollback()
        logging.exception(
            "Failed to send partner withdrawal request %s to admin destination",
            request_model.request_id,
        )
        await callback.answer(_("partners_withdraw_request_create_error"), show_alert=True)
        return

    await state.clear()
    await callback.message.answer(
        _(
            "partners_withdraw_created",
            request_id=request_model.request_id,
            amount=_format_money(request_model.amount),
        )
    )
    await show_partner_dashboard(
        callback,
        settings=settings,
        i18n_data=i18n_data,
        partner_service=partner_service,
        bot=bot,
        session=session,
    )


@router.callback_query(F.data == "partners_action:withdraw_cancel")
async def withdraw_cancel_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    bot: Bot,
    session: AsyncSession,
):
    await state.clear()
    await show_partner_dashboard(
        callback, settings, i18n_data, partner_service, bot, session
    )


@router.callback_query(F.data == "partners_action:change_slug")
async def change_slug_prompt_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    program_settings = await partner_service.get_program_settings(session)
    if not program_settings.is_enabled:
        await callback.answer(_("partners_program_disabled"), show_alert=True)
        return

    await state.set_state(UserPartnerStates.waiting_for_custom_slug)
    await callback.message.edit_text(
        _("partners_enter_new_slug_prompt"),
        reply_markup=get_back_to_main_menu_markup(current_lang, i18n, callback_data="partners_action:open"),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("partners_action:referrals:"))
async def referrals_page_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    try:
        page = int((callback.data or "").split(":")[2])
    except (IndexError, ValueError):
        await callback.answer("Invalid page", show_alert=True)
        return
    await _show_referrals_page(
        callback,
        page=page,
        settings=settings,
        i18n_data=i18n_data,
        partner_service=partner_service,
        session=session,
    )


@router.callback_query(F.data == "partners_action:referrals_noop")
async def referrals_noop_callback(callback: types.CallbackQuery):
    await callback.answer()


@router.message(UserPartnerStates.waiting_for_custom_slug, F.text)
async def process_custom_slug_message(
    message: types.Message,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    bot: Bot,
    session: AsyncSession,
):
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n:
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    program_settings = await partner_service.get_program_settings(session)
    if not program_settings.is_enabled:
        await state.clear()
        await message.answer(_("partners_program_disabled"))
        return

    raw_slug = (message.text or "").strip()
    is_valid, result = await partner_service.validate_custom_slug(
        session, user_id=message.from_user.id, slug=raw_slug
    )
    if not is_valid:
        await message.answer(_(result))
        return

    await partner_service.set_custom_slug(session, user_id=message.from_user.id, slug=result)
    await state.clear()
    await message.answer(_("partners_custom_slug_updated_success"))
    await show_partner_dashboard(
        message, settings, i18n_data, partner_service, bot, session
    )
