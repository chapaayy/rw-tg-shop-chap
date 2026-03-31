import logging
import asyncio
from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.text_decorations import html_decoration as hd
from aiogram.exceptions import TelegramBadRequest
from datetime import datetime, timezone
from typing import Optional, Union, Dict, Any, Callable, Tuple

from config.settings import Settings
from sqlalchemy.orm import sessionmaker
from bot.middlewares.i18n import JsonI18n
from bot.utils.message_queue import get_queue_manager
from bot.utils.text_sanitizer import (
    display_name_or_fallback,
    username_for_display,
)
from bot.utils.telegram_markup import (
    is_profile_link_error,
    remove_profile_link_buttons,
)


class NotificationService:
    """Enhanced notification service for sending messages to admins and log channels"""
    _EVENT_ROUTE_FIELDS: Dict[str, Tuple[str, str]] = {
        "new_users": ("LOG_NEW_USERS_CHAT_ID", "LOG_NEW_USERS_THREAD_ID"),
        "payments": ("LOG_PAYMENTS_CHAT_ID", "LOG_PAYMENTS_THREAD_ID"),
        "promo_activations": (
            "LOG_PROMO_ACTIVATIONS_CHAT_ID",
            "LOG_PROMO_ACTIVATIONS_THREAD_ID",
        ),
        "trial_activations": (
            "LOG_TRIAL_ACTIVATIONS_CHAT_ID",
            "LOG_TRIAL_ACTIVATIONS_THREAD_ID",
        ),
        "suspicious_activity": (
            "LOG_SUSPICIOUS_ACTIVITY_CHAT_ID",
            "LOG_SUSPICIOUS_ACTIVITY_THREAD_ID",
        ),
    }
    
    def __init__(self, bot: Bot, settings: Settings, i18n: Optional[JsonI18n] = None):
        self.bot = bot
        self.settings = settings
        self.i18n = i18n

    def _resolve_default_destination(self) -> Tuple[Optional[int], Optional[int]]:
        default_chat_id = self.settings.LOG_DEFAULT_CHAT_ID
        default_thread_id = self.settings.LOG_DEFAULT_THREAD_ID
        return default_chat_id, default_thread_id

    def _resolve_log_destination(
        self,
        event_type: Optional[str] = None,
        thread_id_override: Optional[int] = None,
    ) -> Tuple[Optional[int], Optional[int]]:
        chat_id, thread_id = self._resolve_default_destination()

        route_fields = self._EVENT_ROUTE_FIELDS.get(event_type or "")
        if route_fields:
            chat_field, thread_field = route_fields
            event_chat_id = getattr(self.settings, chat_field, None)
            event_thread_id = getattr(self.settings, thread_field, None)
            if event_chat_id is not None:
                chat_id = event_chat_id
            if event_thread_id is not None:
                thread_id = event_thread_id

        if thread_id_override is not None:
            thread_id = thread_id_override

        return chat_id, thread_id

    @staticmethod
    def _format_user_display(
        user_id: int,
        username: Optional[str] = None,
        first_name: Optional[str] = None,
    ) -> str:
        base_display = display_name_or_fallback(first_name, f"ID {user_id}")
        if username:
            base_display = f"{base_display} ({username_for_display(username)})"
        return base_display

    @staticmethod
    def _build_profile_keyboard(
        translate: Callable[..., str],
        user_id: int,
        partner_id: Optional[int] = None,
    ) -> InlineKeyboardMarkup:
        """Create inline keyboard with links to user (and related partner) profiles."""
        buttons = [
            [
                InlineKeyboardButton(
                    text=translate(
                        "log_open_profile_link",
                    ),
                    url=f"tg://user?id={user_id}",
                )
            ]
        ]

        if partner_id:
            buttons.append([
                InlineKeyboardButton(
                    text=translate(
                        "log_open_partner_profile_button",
                    ),
                    url=f"tg://user?id={partner_id}",
                )
            ])

        return InlineKeyboardMarkup(inline_keyboard=buttons)
    
    async def _send_to_log_channel(
        self,
        message: str,
        thread_id: Optional[int] = None,
        reply_markup: Optional[InlineKeyboardMarkup] = None,
        event_type: Optional[str] = None,
    ):
        """Send message to configured log channel/group using message queue"""
        resolved_chat_id, final_thread_id = self._resolve_log_destination(
            event_type=event_type,
            thread_id_override=thread_id,
        )
        if resolved_chat_id is None:
            logging.warning(
                "Skipping %s notification: log destination chat is not configured.",
                event_type or "default log",
            )
            return
        
        queue_manager = get_queue_manager()
        if not queue_manager:
            logging.warning("Message queue manager not available, falling back to direct send")

            def _build_kwargs(markup: Optional[InlineKeyboardMarkup]) -> Dict[str, Any]:
                kwargs: Dict[str, Any] = {
                    "chat_id": resolved_chat_id,
                    "text": message,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                }
                if markup:
                    kwargs["reply_markup"] = markup
                if final_thread_id:
                    kwargs["message_thread_id"] = final_thread_id
                return kwargs

            try:
                await self.bot.send_message(**_build_kwargs(reply_markup))
            except TelegramBadRequest as exc:
                if is_profile_link_error(exc):
                    fallback_markup = remove_profile_link_buttons(reply_markup)
                    logging.warning(
                        "Telegram rejected profile buttons for log chat %s: %s. "
                        "Retrying without tg:// links.",
                        resolved_chat_id,
                        getattr(exc, "message", "") or str(exc),
                    )
                    try:
                        await self.bot.send_message(**_build_kwargs(fallback_markup))
                    except Exception as retry_exc:
                        logging.error(
                            "Failed to send notification without profile buttons to log "
                            f"channel {resolved_chat_id}: {retry_exc}"
                        )
                    return
                logging.error(
                    f"Failed to send notification to log channel {resolved_chat_id}: {exc}"
                )
            except Exception as e:
                logging.error(f"Failed to send notification to log channel {resolved_chat_id}: {e}")
            return
        
        try:
            kwargs = {
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            if reply_markup:
                kwargs["reply_markup"] = reply_markup
            
            # Add thread ID for supergroups if specified
            if final_thread_id:
                kwargs["message_thread_id"] = final_thread_id
            
            # Queue message for sending (groups are rate limited to 15/minute)
            await queue_manager.send_message(resolved_chat_id, **kwargs)
            
        except Exception as e:
            logging.error(f"Failed to queue notification to log channel {resolved_chat_id}: {e}")
    
    async def _send_to_admins(self, message: str):
        """Send message to all admin users using message queue"""
        if not self.settings.ADMIN_IDS:
            return
        
        queue_manager = get_queue_manager()
        if not queue_manager:
            logging.warning("Message queue manager not available, falling back to direct send")
            for admin_id in self.settings.ADMIN_IDS:
                try:
                    await self.bot.send_message(
                        chat_id=admin_id,
                        text=message,
                        parse_mode="HTML",
                        disable_web_page_preview=True
                    )
                except Exception as e:
                    logging.error(f"Failed to send notification to admin {admin_id}: {e}")
            return
        
        for admin_id in self.settings.ADMIN_IDS:
            try:
                await queue_manager.send_message(
                    chat_id=admin_id,
                    text=message,
                    parse_mode="HTML",
                    disable_web_page_preview=True
                )
            except Exception as e:
                logging.error(f"Failed to queue notification to admin {admin_id}: {e}")
    
    async def notify_new_user_registration(self, user_id: int, username: Optional[str] = None, 
                                         first_name: Optional[str] = None, 
                                         partner_by_id: Optional[int] = None):
        """Send notification about new user registration"""
        if not self.settings.LOG_NEW_USERS:
            return
        
        admin_lang = self.settings.DEFAULT_LANGUAGE
        _ = lambda k, **kw: self.i18n.gettext(admin_lang, k, **kw) if self.i18n else k
        
        user_display = self._format_user_display(
            user_id=user_id,
            username=username,
            first_name=first_name,
        )
        
        partner_text = ""
        if partner_by_id:
            partner_link = hd.link(str(partner_by_id), f"tg://user?id={partner_by_id}")
            partner_text = _(
                "log_partner_suffix",
                partner_link=partner_link,
            )
        
        message = _(
            "log_new_user_registration",
            user_id=user_id,
            user_display=user_display,
            partner_text=partner_text,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        # Send to log channel
        profile_keyboard = self._build_profile_keyboard(_, user_id, partner_by_id)
        await self._send_to_log_channel(
            message,
            reply_markup=profile_keyboard,
            event_type="new_users",
        )
    
    async def notify_payment_received(self, user_id: int, amount: float, currency: str,
                                    months: int, payment_provider: str, 
                                    username: Optional[str] = None,
                                    traffic_gb: Optional[float] = None):
        """Send notification about successful payment"""
        if not self.settings.LOG_PAYMENTS:
            return
        
        admin_lang = self.settings.DEFAULT_LANGUAGE
        _ = lambda k, **kw: self.i18n.gettext(admin_lang, k, **kw) if self.i18n else k
        
        user_display = self._format_user_display(
            user_id=user_id,
            username=username,
        )
        
        provider_emoji = {
            "yookassa": "💳",
            "freekassa": "💳",
            "cryptopay": "₿",
            "stars": "⭐",
            "platega": "💳",
            "severpay": "💳",
        }.get(payment_provider.lower(), "💰")

        if traffic_gb is not None:
            traffic_label = str(int(traffic_gb)) if float(traffic_gb).is_integer() else f"{traffic_gb:g}"
            message = _(
                "log_payment_received_traffic",
                provider_emoji=provider_emoji,
                user_display=user_display,
                amount=amount,
                currency=currency,
                traffic_gb=traffic_label,
                payment_provider=payment_provider,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        else:
            message = _(
                "log_payment_received",
                provider_emoji=provider_emoji,
                user_display=user_display,
                amount=amount,
                currency=currency,
                months=months,
                payment_provider=payment_provider,
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
        
        # Send to log channel
        profile_keyboard = self._build_profile_keyboard(_, user_id)
        await self._send_to_log_channel(
            message,
            reply_markup=profile_keyboard,
            event_type="payments",
        )
    
    async def notify_promo_activation(self, user_id: int, promo_code: str, bonus_days: int,
                                    username: Optional[str] = None):
        """Send notification about promo code activation"""
        if not self.settings.LOG_PROMO_ACTIVATIONS:
            return

        admin_lang = self.settings.DEFAULT_LANGUAGE
        _ = lambda k, **kw: self.i18n.gettext(admin_lang, k, **kw) if self.i18n else k

        user_display = self._format_user_display(
            user_id=user_id,
            username=username,
        )

        message = _(
            "log_promo_activation",
            user_display=user_display,
            promo_code=promo_code,
            bonus_days=bonus_days,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        # Send to log channel
        profile_keyboard = self._build_profile_keyboard(_, user_id)
        await self._send_to_log_channel(
            message,
            reply_markup=profile_keyboard,
            event_type="promo_activations",
        )

    async def notify_discount_promo_activation(self, user_id: int, promo_code: str, discount_percentage: int,
                                              username: Optional[str] = None):
        """Send notification about discount promo code activation"""
        if not self.settings.LOG_PROMO_ACTIVATIONS:
            return

        admin_lang = self.settings.DEFAULT_LANGUAGE
        _ = lambda k, **kw: self.i18n.gettext(admin_lang, k, **kw) if self.i18n else k

        user_display = self._format_user_display(
            user_id=user_id,
            username=username,
        )

        message = _(
            "log_promo_discount_activation",
            user_display=user_display,
            promo_code=promo_code,
            discount_percentage=discount_percentage,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )

        # Send to log channel
        profile_keyboard = self._build_profile_keyboard(_, user_id)
        await self._send_to_log_channel(
            message,
            reply_markup=profile_keyboard,
            event_type="promo_activations",
        )
    
    async def notify_trial_activation(self, user_id: int, end_date: datetime,
                                    username: Optional[str] = None):
        """Send notification about trial activation"""
        if not self.settings.LOG_TRIAL_ACTIVATIONS:
            return
        
        admin_lang = self.settings.DEFAULT_LANGUAGE
        _ = lambda k, **kw: self.i18n.gettext(admin_lang, k, **kw) if self.i18n else k
        
        user_display = self._format_user_display(
            user_id=user_id,
            username=username,
        )
        
        message = _(
            "log_trial_activation",
            user_display=user_display,
            end_date=end_date.strftime("%Y-%m-%d %H:%M"),
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        
        # Send to log channel
        profile_keyboard = self._build_profile_keyboard(_, user_id)
        await self._send_to_log_channel(
            message,
            reply_markup=profile_keyboard,
            event_type="trial_activations",
        )

    async def notify_panel_sync(self, status: str, details: str, 
                               users_processed: int, subs_synced: int,
                               username: Optional[str] = None):
        """Send notification about panel synchronization"""
        if not getattr(self.settings, 'LOG_PANEL_SYNC', True):
            return
        
        admin_lang = self.settings.DEFAULT_LANGUAGE
        _ = lambda k, **kw: self.i18n.gettext(admin_lang, k, **kw) if self.i18n else k
        
        # Status emoji based on sync result
        status_emoji = {
            "completed": "✅",
            "completed_with_errors": "⚠️", 
            "failed": "❌"
        }.get(status, "🔄")
        
        message = _(
            "log_panel_sync",
            status_emoji=status_emoji,
            status=status,
            users_processed=users_processed,
            subs_synced=subs_synced,
            timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z"),
            details=details
        )
        
        # Send to log channel
        await self._send_to_log_channel(message)

    async def notify_suspicious_promo_attempt(
            self, user_id: int, suspicious_input: str,
            username: Optional[str] = None, first_name: Optional[str] = None):
        """Send notification about a suspicious promo code attempt."""
        if not self.settings.LOG_SUSPICIOUS_ACTIVITY:
            return

        admin_lang = self.settings.DEFAULT_LANGUAGE
        _ = lambda k, **kw: self.i18n.gettext(
            admin_lang, k, **kw) if self.i18n else k

        user_display = self._format_user_display(
            user_id=user_id,
            username=username,
            first_name=first_name,
        )

        message = _(
            "log_suspicious_promo",
            user_display=hd.quote(user_display),
            user_id=user_id,
            suspicious_input=hd.quote(suspicious_input),
            timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S %Z"))

        # Send to log channel
        profile_keyboard = self._build_profile_keyboard(_, user_id)
        await self._send_to_log_channel(
            message,
            reply_markup=profile_keyboard,
            event_type="suspicious_activity",
        )
    
    async def send_custom_notification(self, message: str, to_admins: bool = False, 
                                     to_log_channel: bool = True, thread_id: Optional[int] = None,
                                     event_type: Optional[str] = None):
        """Send custom notification message"""
        if to_log_channel:
            await self._send_to_log_channel(message, thread_id, event_type=event_type)
        if to_admins:
            await self._send_to_admins(message)

# Removed helper functions that duplicated NotificationService API
