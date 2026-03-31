import logging
from typing import Optional, Tuple

from aiogram import F, Router, types
from aiogram.fsm.context import FSMContext
from aiogram.utils.keyboard import InlineKeyboardBuilder, InlineKeyboardButton
from aiogram.utils.markdown import hcode
from sqlalchemy.ext.asyncio import AsyncSession

from bot.middlewares.i18n import JsonI18n
from bot.services.partner_service import PartnerService
from bot.states.admin_states import AdminStates
from config.settings import Settings
from db.dal import partner_dal


router = Router(name="admin_partners_router")

PAGE_SIZE = 10
MAX_PERCENT = 100.0


def _translator(i18n_data: dict, settings: Settings) -> Tuple[Optional[JsonI18n], str]:
    lang = i18n_data.get("current_language", settings.DEFAULT_LANGUAGE)
    i18n = i18n_data.get("i18n_instance")
    return i18n, lang


def _extract_partner_and_page(callback_data: Optional[str], expected_action: str) -> Tuple[int, int]:
    parts = (callback_data or "").split(":")
    if len(parts) == 3:
        scope, action, raw_partner_id = parts
        raw_page = "0"
    elif len(parts) == 4:
        scope, action, raw_partner_id, raw_page = parts
    else:
        raise ValueError("Invalid callback format")
    if scope != "admin_partners" or action != expected_action:
        raise ValueError("Invalid callback payload")
    partner_user_id = int(raw_partner_id)
    back_page = int(raw_page) if raw_page else 0
    return partner_user_id, max(0, back_page)


def _fmt_username(user) -> str:
    if user.username:
        return f"@{user.username}"
    if user.first_name:
        return user.first_name
    return f"ID {user.user_id}"


def _fmt_referral_user(row: dict) -> str:
    username = (row.get("username") or "").strip()
    first_name = (row.get("first_name") or "").strip()
    invited_user_id = row.get("invited_user_id")
    if username:
        return f"@{username}"
    if first_name:
        return first_name
    return f"ID {invited_user_id}"


def _build_partner_program_menu_keyboard(_) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_("admin_partners_list_button"),
            callback_data="admin_partners:open_list",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=_("admin_partners_settings_button"),
            callback_data="admin_partners:open_settings",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=_("back_to_admin_panel_button"),
            callback_data="admin_action:main",
        )
    )
    return builder.as_markup()


def _build_partners_list_keyboard(
    _,
    *,
    rows,
    current_page: int,
    total_pages: int,
) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    for account, user in rows:
        status_icon = "\u2705" if account.is_enabled else "\u26d4"
        percent_text = (
            f"{float(account.personal_percent):.2f}%"
            if account.personal_percent is not None
            else "AUTO"
        )
        builder.row(
            InlineKeyboardButton(
                text=f"{status_icon} {_fmt_username(user)} \u2022 {percent_text}",
                callback_data=f"admin_partners:card:{user.user_id}:{current_page}",
            )
        )

    nav = []
    if current_page > 0:
        nav.append(
            InlineKeyboardButton(
                text="\u2b05\ufe0f",
                callback_data=f"admin_partners:page:{current_page - 1}",
            )
        )
    nav.append(
        InlineKeyboardButton(
            text=f"{current_page + 1}/{total_pages}",
            callback_data="admin_partners:noop",
        )
    )
    if current_page < total_pages - 1:
        nav.append(
            InlineKeyboardButton(
                text="\u27a1\ufe0f",
                callback_data=f"admin_partners:page:{current_page + 1}",
            )
        )
    builder.row(*nav)

    builder.row(
        InlineKeyboardButton(
            text=_("admin_partner_back_to_program_menu_button"),
            callback_data="admin_partners:menu",
        )
    )
    return builder.as_markup()

def _build_partner_card_keyboard(
    _,
    *,
    partner_user_id: int,
    enabled: bool,
    has_personal_percent: bool,
    back_page: int,
) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_("admin_partner_toggle_disable_button")
            if enabled
            else _("admin_partner_toggle_enable_button"),
            callback_data=f"admin_partners:toggle:{partner_user_id}:{back_page}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=_("admin_partner_set_percent_button"),
            callback_data=f"admin_partners:set_percent_prompt:{partner_user_id}:{back_page}",
        )
    )
    if has_personal_percent:
        builder.row(
            InlineKeyboardButton(
                text=_("admin_partner_clear_percent_button"),
                callback_data=f"admin_partners:clear_percent:{partner_user_id}:{back_page}",
            )
        )
    builder.row(
        InlineKeyboardButton(
            text=_("admin_partner_view_referrals_button"),
            callback_data=f"admin_partners:referrals:{partner_user_id}:{back_page}",
        ),
        InlineKeyboardButton(
            text=_("admin_partner_view_commissions_button"),
            callback_data=f"admin_partners:commissions:{partner_user_id}:{back_page}",
        ),
    )
    builder.row(
        InlineKeyboardButton(
            text=_("admin_partner_back_to_list_button"),
            callback_data=f"admin_partners:page:{back_page}",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=_("back_to_admin_panel_button"),
            callback_data="admin_action:main",
        )
    )
    return builder.as_markup()


def _build_partner_settings_keyboard(_, settings_model) -> types.InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=_("admin_partner_settings_toggle_program"),
            callback_data="admin_partners:settings_toggle_program",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=_("admin_partner_settings_toggle_traffic"),
            callback_data="admin_partners:settings_toggle_traffic",
        )
    )
    builder.row(
        InlineKeyboardButton(
            text=_("admin_partner_settings_set_default_percent"),
            callback_data="admin_partners:settings_set_default_percent",
        ),
        InlineKeyboardButton(
            text=_("admin_partner_settings_set_min_payment"),
            callback_data="admin_partners:settings_set_min_payment",
        ),
    )
    builder.row(
        InlineKeyboardButton(
            text=_("admin_partner_back_to_program_menu_button"),
            callback_data="admin_partners:menu",
        )
    )
    return builder.as_markup()


async def _render_partner_program_menu(
    callback: types.CallbackQuery,
    *,
    settings: Settings,
    i18n_data: dict,
) -> None:
    i18n, lang = _translator(i18n_data, settings)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    await callback.message.edit_text(
        _("admin_partner_program_menu_title"),
        parse_mode="HTML",
        reply_markup=_build_partner_program_menu_keyboard(_),
    )
    await callback.answer()


async def _render_partners_menu(
    callback: types.CallbackQuery,
    *,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
    page: int,
) -> None:
    i18n, lang = _translator(i18n_data, settings)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    safe_page = max(0, page)
    created_accounts = await partner_dal.ensure_accounts_for_existing_users(session)
    if created_accounts:
        logging.info("Partner accounts bootstrap created %s rows.", created_accounts)

    settings_model = await partner_dal.get_or_create_program_settings(session)
    total = await partner_dal.count_partners(session)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    if safe_page >= total_pages:
        safe_page = total_pages - 1
    rows = await partner_dal.list_partners_paged(
        session, page=safe_page, page_size=PAGE_SIZE
    )

    header = _(
        "admin_partners_header",
        total=total,
        current_page=safe_page + 1,
        total_pages=total_pages,
        is_enabled=_("admin_partner_status_enabled")
        if settings_model.is_enabled
        else _("admin_partner_status_disabled"),
        default_percent=f"{float(settings_model.default_percent):.2f}",
        allow_traffic=_("yes_button")
        if settings_model.allow_traffic_commission
        else _("no_button"),
        min_payment=f"{float(settings_model.min_payment_amount):.2f}",
    )

    if not rows:
        text = f"{header}\n\n{_('admin_partners_empty')}"
    else:
        text = header

    markup = _build_partners_list_keyboard(
        _,
        rows=rows,
        current_page=safe_page,
        total_pages=total_pages,
    )
    await callback.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
    await callback.answer()


def _partner_card_text(_, data: dict) -> str:
    user = data["user"]
    account = data["account"]
    return _(
        "admin_partner_card_text",
        user_id=user.user_id,
        username=hcode(_fmt_username(user)),
        default_slug=hcode(account.default_slug),
        custom_slug=hcode(account.custom_slug or _("admin_partner_custom_slug_not_set")),
        active_slug=hcode(account.custom_slug or account.default_slug),
        is_enabled=_("admin_partner_status_enabled")
        if account.is_enabled
        else _("admin_partner_status_disabled"),
        personal_percent=(
            f"{float(account.personal_percent):.2f}%"
            if account.personal_percent is not None
            else _("admin_partner_percent_auto")
        ),
        effective_percent=f"{float(data['effective_percent']):.2f}",
        invited_count=data["invited_count"],
        paid_count=data["paid_count"],
        turnover=f"{float(data['turnover']):.2f}",
        income=f"{float(data['income']):.2f}",
    )


async def _show_partner_card(
    callback: types.CallbackQuery,
    *,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
    partner_user_id: int,
    back_page: int,
) -> None:
    i18n, lang = _translator(i18n_data, settings)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    data = await partner_service.get_partner_admin_card_data(session, partner_user_id)
    if not data:
        await callback.answer(_("admin_partner_not_found"), show_alert=True)
        return

    markup = _build_partner_card_keyboard(
        _,
        partner_user_id=partner_user_id,
        enabled=bool(data["account"].is_enabled),
        has_personal_percent=data["account"].personal_percent is not None,
        back_page=max(back_page, 0),
    )
    await callback.message.edit_text(
        _partner_card_text(_, data),
        reply_markup=markup,
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "admin_partners:menu")
async def show_partner_program_menu(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
):
    await _render_partner_program_menu(
        callback, settings=settings, i18n_data=i18n_data
    )


@router.callback_query(F.data == "admin_partners:open_list")
async def show_partners_menu(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
):
    await _render_partners_menu(
        callback, settings=settings, i18n_data=i18n_data, session=session, page=0
    )


@router.callback_query(F.data == "admin_partners:open_settings")
async def open_partner_settings_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
):
    await partner_settings_menu_callback(callback, settings, i18n_data, session)


@router.callback_query(F.data.startswith("admin_partners:page:"))
async def partners_page_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
):
    try:
        page = int((callback.data or "").split(":")[2])
    except (IndexError, ValueError):
        await callback.answer("Invalid page", show_alert=True)
        return
    await _render_partners_menu(
        callback, settings=settings, i18n_data=i18n_data, session=session, page=page
    )


@router.callback_query(F.data.startswith("admin_partners:card:"))
async def partner_card_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    try:
        partner_user_id, back_page = _extract_partner_and_page(
            callback.data, "card"
        )
    except (ValueError, IndexError):
        await callback.answer("Invalid partner", show_alert=True)
        return

    await _show_partner_card(
        callback,
        settings=settings,
        i18n_data=i18n_data,
        partner_service=partner_service,
        session=session,
        partner_user_id=partner_user_id,
        back_page=back_page,
    )


@router.callback_query(F.data.startswith("admin_partners:toggle:"))
async def partner_toggle_enabled_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    i18n, lang = _translator(i18n_data, settings)
    if not i18n:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    try:
        partner_user_id, back_page = _extract_partner_and_page(
            callback.data, "toggle"
        )
    except (ValueError, IndexError):
        await callback.answer("Invalid partner", show_alert=True)
        return

    account = await partner_dal.ensure_partner_account(session, partner_user_id)
    await partner_dal.set_partner_enabled(session, partner_user_id, not account.is_enabled)
    await callback.answer(_("admin_partner_settings_updated"), show_alert=True)
    await _show_partner_card(
        callback,
        settings=settings,
        i18n_data=i18n_data,
        partner_service=partner_service,
        session=session,
        partner_user_id=partner_user_id,
        back_page=back_page,
    )


@router.callback_query(F.data.startswith("admin_partners:set_percent_prompt:"))
async def partner_set_percent_prompt_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
):
    i18n, lang = _translator(i18n_data, settings)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)
    try:
        partner_user_id, back_page = _extract_partner_and_page(
            callback.data, "set_percent_prompt"
        )
    except (ValueError, IndexError):
        await callback.answer("Invalid partner", show_alert=True)
        return

    await state.update_data(
        partner_user_id=partner_user_id,
        partner_back_page=back_page,
    )
    await state.set_state(AdminStates.waiting_for_partner_percent)
    await callback.message.edit_text(
        _("admin_partner_percent_prompt"),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(AdminStates.waiting_for_partner_percent, F.text)
async def partner_set_percent_message(
    message: types.Message,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    i18n, lang = _translator(i18n_data, settings)
    if not i18n:
        await state.clear()
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    state_data = await state.get_data()
    partner_user_id = state_data.get("partner_user_id")
    back_page = int(state_data.get("partner_back_page", 0))
    if not partner_user_id:
        await state.clear()
        return

    raw_value = (message.text or "").strip().lower()
    if raw_value in {"clear", "none", "reset", "сброс", "нет"}:
        await partner_dal.set_partner_personal_percent(session, int(partner_user_id), None)
        await message.answer(_("admin_partner_percent_cleared"))
    else:
        try:
            value = float(raw_value.replace(",", "."))
        except ValueError:
            await message.answer(_("admin_partner_percent_invalid"))
            return
        if value < 0 or value > MAX_PERCENT:
            await message.answer(_("admin_partner_percent_invalid"))
            return
        await partner_dal.set_partner_personal_percent(
            session, int(partner_user_id), value
        )
        await message.answer(_("admin_partner_percent_updated", percent=f"{value:.2f}"))

    await state.clear()
    data = await partner_service.get_partner_admin_card_data(session, int(partner_user_id))
    if not data:
        await message.answer(_("admin_partner_not_found"))
        return
    markup = _build_partner_card_keyboard(
        _,
        partner_user_id=int(partner_user_id),
        enabled=bool(data["account"].is_enabled),
        has_personal_percent=data["account"].personal_percent is not None,
        back_page=back_page,
    )
    await message.answer(
        _partner_card_text(_, data),
        parse_mode="HTML",
        reply_markup=markup,
    )


@router.callback_query(F.data.startswith("admin_partners:clear_percent:"))
async def partner_clear_percent_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    i18n, lang = _translator(i18n_data, settings)
    if not i18n:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    try:
        partner_user_id, back_page = _extract_partner_and_page(
            callback.data, "clear_percent"
        )
    except (ValueError, IndexError):
        await callback.answer("Invalid partner", show_alert=True)
        return

    await partner_dal.set_partner_personal_percent(session, partner_user_id, None)
    await callback.answer(_("admin_partner_percent_cleared"), show_alert=True)
    await _show_partner_card(
        callback,
        settings=settings,
        i18n_data=i18n_data,
        partner_service=partner_service,
        session=session,
        partner_user_id=partner_user_id,
        back_page=back_page,
    )


@router.callback_query(F.data.startswith("admin_partners:referrals:"))
async def partner_referrals_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    i18n, lang = _translator(i18n_data, settings)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    try:
        partner_user_id, back_page = _extract_partner_and_page(
            callback.data, "referrals"
        )
    except (ValueError, IndexError):
        await callback.answer("Invalid partner", show_alert=True)
        return

    data = await partner_service.get_partner_admin_card_data(session, partner_user_id)
    if not data:
        await callback.answer(_("admin_partner_not_found"), show_alert=True)
        return

    referrals = data.get("referrals") or []
    if not referrals:
        text = _("admin_partner_referrals_empty")
    else:
        lines = [_("admin_partner_referrals_title")]
        for row in referrals:
            lines.append(
                _(
                    "admin_partner_referrals_item",
                    invited_user_id=row["invited_user_id"],
                    user=hcode(_fmt_referral_user(row)),
                    payments_count=row["payments_count"],
                    turnover=f"{float(row['turnover']):.2f}",
                    income=f"{float(row['income']):.2f}",
                )
            )
        text = "\n".join(lines)

    back_markup = InlineKeyboardBuilder()
    back_markup.row(
        InlineKeyboardButton(
            text=_("admin_partner_back_to_list_button"),
            callback_data=f"admin_partners:card:{partner_user_id}:{back_page}",
        )
    )
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=back_markup.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("admin_partners:commissions:"))
async def partner_commissions_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    partner_service: PartnerService,
    session: AsyncSession,
):
    i18n, lang = _translator(i18n_data, settings)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    try:
        partner_user_id, back_page = _extract_partner_and_page(
            callback.data, "commissions"
        )
    except (ValueError, IndexError):
        await callback.answer("Invalid partner", show_alert=True)
        return

    data = await partner_service.get_partner_admin_card_data(session, partner_user_id)
    if not data:
        await callback.answer(_("admin_partner_not_found"), show_alert=True)
        return

    commissions = data.get("commissions") or []
    if not commissions:
        text = _("admin_partner_commissions_empty")
    else:
        lines = [_("admin_partner_commissions_title")]
        for item in commissions:
            lines.append(
                _(
                    "admin_partner_commissions_item",
                    payment_id=item.payment_id,
                    invited_user_id=item.invited_user_id,
                    payment_amount=f"{float(item.payment_amount):.2f}",
                    percent=f"{float(item.percent_applied):.2f}",
                    commission_amount=f"{float(item.commission_amount):.2f}",
                    currency=item.currency,
                    created_at=item.created_at.strftime("%Y-%m-%d %H:%M")
                    if item.created_at
                    else "-",
                )
            )
        text = "\n".join(lines)

    back_markup = InlineKeyboardBuilder()
    back_markup.row(
        InlineKeyboardButton(
            text=_("admin_partner_back_to_list_button"),
            callback_data=f"admin_partners:card:{partner_user_id}:{back_page}",
        )
    )
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=back_markup.as_markup())
    await callback.answer()


@router.callback_query(F.data == "admin_partners:settings")
async def partner_settings_menu_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
):
    i18n, lang = _translator(i18n_data, settings)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    settings_model = await partner_dal.get_or_create_program_settings(session)
    text = _(
        "admin_partner_settings_title",
        is_enabled=_("admin_partner_status_enabled")
        if settings_model.is_enabled
        else _("admin_partner_status_disabled"),
        default_percent=f"{float(settings_model.default_percent):.2f}",
        allow_traffic=_("yes_button")
        if settings_model.allow_traffic_commission
        else _("no_button"),
        min_payment=f"{float(settings_model.min_payment_amount):.2f}",
    )
    markup = _build_partner_settings_keyboard(_, settings_model)
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data == "admin_partners:settings_toggle_program")
async def partner_settings_toggle_program_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
):
    current = await partner_dal.get_or_create_program_settings(session)
    await partner_dal.update_program_settings(
        session, is_enabled=not bool(current.is_enabled)
    )
    await partner_settings_menu_callback(callback, settings, i18n_data, session)


@router.callback_query(F.data == "admin_partners:settings_toggle_traffic")
async def partner_settings_toggle_traffic_callback(
    callback: types.CallbackQuery,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
):
    current = await partner_dal.get_or_create_program_settings(session)
    await partner_dal.update_program_settings(
        session, allow_traffic_commission=not bool(current.allow_traffic_commission)
    )
    await partner_settings_menu_callback(callback, settings, i18n_data, session)


@router.callback_query(F.data == "admin_partners:settings_set_default_percent")
async def partner_settings_default_percent_prompt_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
):
    i18n, lang = _translator(i18n_data, settings)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)
    await state.set_state(AdminStates.waiting_for_partner_default_percent)
    await callback.message.edit_text(_("admin_partner_settings_default_percent_prompt"))
    await callback.answer()


@router.message(AdminStates.waiting_for_partner_default_percent, F.text)
async def partner_settings_default_percent_message(
    message: types.Message,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
):
    i18n, lang = _translator(i18n_data, settings)
    if not i18n:
        await state.clear()
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    raw_value = (message.text or "").strip().replace(",", ".")
    try:
        value = float(raw_value)
    except ValueError:
        await message.answer(_("admin_partner_settings_invalid_percent"))
        return
    if value < 0 or value > MAX_PERCENT:
        await message.answer(_("admin_partner_settings_invalid_percent"))
        return

    await partner_dal.update_program_settings(session, default_percent=value)
    await state.clear()
    await message.answer(_("admin_partner_settings_updated"))


@router.callback_query(F.data == "admin_partners:settings_set_min_payment")
async def partner_settings_min_payment_prompt_callback(
    callback: types.CallbackQuery,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
):
    i18n, lang = _translator(i18n_data, settings)
    if not i18n or not callback.message:
        await callback.answer("Language error", show_alert=True)
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)
    await state.set_state(AdminStates.waiting_for_partner_min_payment)
    await callback.message.edit_text(_("admin_partner_settings_min_payment_prompt"))
    await callback.answer()


@router.message(AdminStates.waiting_for_partner_min_payment, F.text)
async def partner_settings_min_payment_message(
    message: types.Message,
    state: FSMContext,
    settings: Settings,
    i18n_data: dict,
    session: AsyncSession,
):
    i18n, lang = _translator(i18n_data, settings)
    if not i18n:
        await state.clear()
        return
    _ = lambda key, **kwargs: i18n.gettext(lang, key, **kwargs)

    raw_value = (message.text or "").strip().replace(",", ".")
    try:
        value = float(raw_value)
    except ValueError:
        await message.answer(_("admin_partner_settings_invalid_amount"))
        return
    if value < 0:
        await message.answer(_("admin_partner_settings_invalid_amount"))
        return

    await partner_dal.update_program_settings(session, min_payment_amount=value)
    await state.clear()
    await message.answer(_("admin_partner_settings_updated"))


@router.callback_query(F.data == "admin_partners:noop")
async def partner_noop_callback(callback: types.CallbackQuery):
    await callback.answer()

