import logging
from typing import Dict, Optional

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.storage.redis import DefaultKeyBuilder, RedisStorage
from sqlalchemy.orm import sessionmaker

from config.settings import Settings
from bot.middlewares.callback_guard import CallbackGuardMiddleware
from bot.middlewares.db_session import DBSessionMiddleware
from bot.middlewares.i18n import I18nMiddleware, get_i18n_instance, JsonI18n
from bot.middlewares.ban_check_middleware import BanCheckMiddleware
from bot.middlewares.action_logger_middleware import ActionLoggerMiddleware
from bot.middlewares.profile_sync import ProfileSyncMiddleware
from bot.middlewares.channel_subscription import ChannelSubscriptionMiddleware
from bot.services.redis_service import RedisService


def build_dispatcher(
    settings: Settings,
    async_session_factory: sessionmaker,
    redis_service: Optional[RedisService] = None,
) -> tuple[Dispatcher, Bot, Dict]:
    storage = MemoryStorage()
    if redis_service and redis_service.is_available() and redis_service.client:
        try:
            storage = RedisStorage(
                redis=redis_service.client,
                key_builder=DefaultKeyBuilder(
                    prefix=f"{settings.REDIS_KEY_PREFIX}:fsm",
                    with_destiny=True,
                ),
                state_ttl=max(1, int(settings.REDIS_FSM_STATE_TTL_SECONDS)),
                data_ttl=max(1, int(settings.REDIS_FSM_DATA_TTL_SECONDS)),
            )
            logging.info("FSM storage configured: RedisStorage.")
        except Exception as exc:
            logging.error(
                "Failed to initialize Redis FSM storage, falling back to MemoryStorage: %s",
                exc,
                exc_info=True,
            )
            storage = MemoryStorage()
    else:
        logging.info("FSM storage configured: MemoryStorage.")

    default_props = DefaultBotProperties(parse_mode=ParseMode.HTML)

    session = None
    if settings.TELEGRAM_PROXY_URL:
        session = AiohttpSession(proxy=settings.TELEGRAM_PROXY_URL)
        logging.info("Telegram Bot API proxy configured: %s", settings.TELEGRAM_PROXY_URL)

    bot = Bot(token=settings.BOT_TOKEN, default=default_props, session=session)

    dp = Dispatcher(storage=storage, settings=settings, bot_instance=bot)

    i18n_instance = get_i18n_instance(path="locales", default=settings.DEFAULT_LANGUAGE)

    dp["i18n_instance"] = i18n_instance
    dp["async_session_factory"] = async_session_factory
    dp["redis_service"] = redis_service

    dp.update.outer_middleware(DBSessionMiddleware(async_session_factory))
    dp.update.outer_middleware(I18nMiddleware(i18n=i18n_instance, settings=settings))
    dp.update.outer_middleware(
        CallbackGuardMiddleware(
            settings=settings,
            i18n_instance=i18n_instance,
            redis_service=redis_service,
        )
    )
    dp.update.outer_middleware(ProfileSyncMiddleware())
    dp.update.outer_middleware(BanCheckMiddleware(settings=settings, i18n_instance=i18n_instance))
    dp.update.outer_middleware(ChannelSubscriptionMiddleware(settings=settings, i18n_instance=i18n_instance))
    dp.update.outer_middleware(ActionLoggerMiddleware(settings=settings))

    return dp, bot, {"i18n_instance": i18n_instance}
