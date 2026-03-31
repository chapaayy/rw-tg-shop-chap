import logging
from typing import Optional, Union

from aiogram import F, Router, types, Bot
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardButton
from sqlalchemy.ext.asyncio import AsyncSession

from bot.middlewares.i18n import JsonI18n
from bot.services.partner_service import PartnerService
from bot.states.user_states import UserPartnerStates
from bot.utils.text_sanitizer import sanitize_display_name, sanitize_username
from bot.keyboards.inline.user_keyboards import (
    get_back_to_main_menu_markup,
    get_partner_menu_keyboard,
)
from config.settings import Settings

router = Router(name="user_partners_router")
REFERRALS_PAGE_SIZE = 10


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


def _compose_dashboard_text(_: callable, dashboard: dict) -> str:
    return _(
        "partners_dashboard_header",
        active_link=dashboard["active_link"],
        percent=f"{dashboard['effective_percent']:.2f}",
        invited_count=dashboard["invited_count"],
        paid_count=dashboard["paid_count"],
        turnover=f"{dashboard['turnover']:.2f}",
        income=f"{dashboard['income']:.2f}",
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
