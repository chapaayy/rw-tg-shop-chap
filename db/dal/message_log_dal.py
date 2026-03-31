import logging
from typing import Optional, List
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, or_

from ..models import MessageLog, User


async def create_message_log(session: AsyncSession,
                             log_data: dict) -> Optional[MessageLog]:

    try:
        log_entry = await create_message_log_no_commit(session, log_data)
        await session.commit()
        await session.refresh(log_entry)
        return log_entry
    except Exception as e:
        await session.rollback()
        logging.error(f"Failed to create and commit message log: {e}",
                      exc_info=True)
        return None


async def get_all_message_logs(session: AsyncSession,
                               limit: int,
                               offset: int,
                               hide_admin_events: bool = False) -> List[MessageLog]:
    stmt = select(MessageLog)
    if hide_admin_events:
        stmt = stmt.where(MessageLog.is_admin_event.is_(False))
    stmt = stmt.order_by(MessageLog.timestamp.desc()).limit(limit).offset(offset)
    result = await session.execute(stmt)
    return result.scalars().all()


async def count_all_message_logs(session: AsyncSession,
                                  hide_admin_events: bool = False) -> int:
    stmt = select(func.count()).select_from(MessageLog)
    if hide_admin_events:
        stmt = stmt.where(MessageLog.is_admin_event.is_(False))
    result = await session.execute(stmt)
    return result.scalar_one()


async def get_user_message_logs(session: AsyncSession, user_id_to_search: int,
                                limit: int, offset: int) -> List[MessageLog]:
    stmt = (select(MessageLog).where(
        or_(MessageLog.user_id == user_id_to_search,
            MessageLog.target_user_id == user_id_to_search)).order_by(
                MessageLog.timestamp.desc()).limit(limit).offset(offset))
    result = await session.execute(stmt)
    return result.scalars().all()


async def count_user_message_logs(session: AsyncSession,
                                  user_id_to_search: int) -> int:
    stmt = (select(func.count()).select_from(MessageLog).where(
        or_(MessageLog.user_id == user_id_to_search,
            MessageLog.target_user_id == user_id_to_search)))
    result = await session.execute(stmt)
    return result.scalar_one()


async def create_message_log_no_commit(session: AsyncSession,
                                       log_data: dict) -> MessageLog:
    author_user_id = log_data.get("user_id")
    if author_user_id is not None:
        author_exists = await session.execute(
            select(User.user_id).where(User.user_id == author_user_id)
        )
        if author_exists.scalar_one_or_none() is None:
            logging.warning(
                "Author user %s not found for message log. Setting to NULL.",
                author_user_id,
            )
            log_data["user_id"] = None

    target_user_id = log_data.get("target_user_id")
    if target_user_id is not None:
        target_exists = await session.execute(
            select(User.user_id).where(User.user_id == target_user_id)
        )
        if target_exists.scalar_one_or_none() is None:
            logging.warning(
                "Target user %s not found for message log. Setting to NULL.",
                target_user_id,
            )
            log_data["target_user_id"] = None

    new_log = MessageLog(**log_data)
    session.add(new_log)

    logging.debug(
        f"Message log added to session: user {log_data.get('user_id')}, event {log_data.get('event_type')}"
    )
    return new_log
