import logging
from typing import Optional, Union

from aiogram import F, Router, types, Bot
from aiogram.fsm.context import FSMContext
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
    account = dashboard["account"]
    lines = [
        _(
            "partners_dashboard_header",
            active_link=dashboard["active_link"],
            default_link=dashboard["default_link"],
            custom_link=dashboard["custom_link"] or _("partners_custom_link_not_set"),
            active_slug=account.custom_slug or account.default_slug,
            default_slug=account.default_slug,
            custom_slug=account.custom_slug or _("partners_custom_link_not_set"),
            percent=f"{dashboard['effective_percent']:.2f}",
            invited_count=dashboard["invited_count"],
            paid_count=dashboard["paid_count"],
            turnover=f"{dashboard['turnover']:.2f}",
            income=f"{dashboard['income']:.2f}",
        )
    ]

    rows = dashboard.get("referrals") or []
    if rows:
        lines.append(_("partners_referrals_list_header"))
        for row in rows[:20]:
            invited_user_id = int(row.get("invited_user_id"))
            user_label = _format_referral_name(row, invited_user_id)
            lines.append(
                _(
                    "partners_referral_list_item",
                    user=user_label,
                    invited_user_id=invited_user_id,
                    payments_count=int(row.get("payments_count") or 0),
                    turnover=f"{float(row.get('turnover') or 0.0):.2f}",
                    income=f"{float(row.get('income') or 0.0):.2f}",
                )
            )
    else:
        lines.append(_("partners_referrals_list_empty"))

    return "\n".join(lines)


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
        session, user_id=event.from_user.id, bot_username=bot_username
    )
    text = _compose_dashboard_text(_, dashboard)
    keyboard = get_partner_menu_keyboard(current_lang, i18n, has_custom_slug=bool(dashboard["account"].custom_slug))

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


@router.callback_query(F.data == "partners_action:reset_slug")
async def reset_slug_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    bot: Bot,
    session: AsyncSession,
):
    current_lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n: Optional[JsonI18n] = i18n_data.get("i18n_instance")
    if not i18n:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(current_lang, key, **kwargs)

    program_settings = await partner_service.get_program_settings(session)
    if not program_settings.is_enabled:
        await callback.answer(_("partners_program_disabled"), show_alert=True)
        return

    await partner_service.clear_custom_slug(session, user_id=callback.from_user.id)
    await callback.answer(_("partners_custom_slug_reset_alert"), show_alert=True)
    await show_partner_dashboard(callback, settings, i18n_data, partner_service, bot, session)


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
