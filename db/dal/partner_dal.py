import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import (
    PartnerAccount,
    PartnerCommission,
    PartnerWithdrawalRequest,
    PartnerProgramSettings,
    PartnerReferral,
    Payment,
    User,
)


def _normalize_slug(value: str) -> str:
    return (value or "").strip().lower()


def build_default_slug(user_id: int) -> str:
    return f"u{user_id}"


PARTNER_WITHDRAWAL_METHOD_SBP = "sbp"
PARTNER_WITHDRAWAL_METHOD_USDT_TRC20 = "usdt_trc20"

PARTNER_WITHDRAWAL_STATUS_PENDING = "pending"
PARTNER_WITHDRAWAL_STATUS_APPROVED = "approved"
PARTNER_WITHDRAWAL_STATUS_REJECTED = "rejected"
PARTNER_WITHDRAWAL_STATUS_PAID = "paid"
PARTNER_WITHDRAWAL_STATUS_CANCELLED = "cancelled"

PARTNER_WITHDRAWAL_ACTIVE_STATUSES = (
    PARTNER_WITHDRAWAL_STATUS_PENDING,
    PARTNER_WITHDRAWAL_STATUS_APPROVED,
)
PARTNER_WITHDRAWAL_RESERVED_STATUSES = PARTNER_WITHDRAWAL_ACTIVE_STATUSES
PARTNER_WITHDRAWAL_IN_PROCESS_STATUSES = (PARTNER_WITHDRAWAL_STATUS_APPROVED,)


async def get_or_create_program_settings(session: AsyncSession) -> PartnerProgramSettings:
    model = await session.get(PartnerProgramSettings, 1)
    if model:
        return model
    model = PartnerProgramSettings(id=1)
    session.add(model)
    await session.flush()
    await session.refresh(model)
    return model


async def update_program_settings(
    session: AsyncSession,
    *,
    is_enabled: Optional[bool] = None,
    default_percent: Optional[float] = None,
    allow_traffic_commission: Optional[bool] = None,
    min_payment_amount: Optional[float] = None,
) -> PartnerProgramSettings:
    model = await get_or_create_program_settings(session)
    if is_enabled is not None:
        model.is_enabled = bool(is_enabled)
    if default_percent is not None:
        model.default_percent = float(default_percent)
    if allow_traffic_commission is not None:
        model.allow_traffic_commission = bool(allow_traffic_commission)
    if min_payment_amount is not None:
        model.min_payment_amount = float(min_payment_amount)
    await session.flush()
    await session.refresh(model)
    return model


async def get_partner_account(session: AsyncSession, user_id: int) -> Optional[PartnerAccount]:
    stmt = select(PartnerAccount).where(PartnerAccount.user_id == user_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def ensure_partner_account(session: AsyncSession, user_id: int) -> PartnerAccount:
    existing = await get_partner_account(session, user_id)
    if existing:
        return existing

    default_slug = build_default_slug(user_id)
    model = PartnerAccount(user_id=user_id, default_slug=default_slug)
    session.add(model)
    await session.flush()
    await session.refresh(model)
    return model


async def get_partner_by_slug(session: AsyncSession, slug: str) -> Optional[PartnerAccount]:
    normalized = _normalize_slug(slug)
    if not normalized:
        return None
    stmt = select(PartnerAccount).where(
        or_(
            PartnerAccount.default_slug == normalized,
            PartnerAccount.custom_slug == normalized,
        )
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def is_slug_available(
    session: AsyncSession,
    slug: str,
    *,
    exclude_user_id: Optional[int] = None,
) -> bool:
    normalized = _normalize_slug(slug)
    if not normalized:
        return False
    conditions = [
        or_(
            PartnerAccount.default_slug == normalized,
            PartnerAccount.custom_slug == normalized,
        )
    ]
    if exclude_user_id is not None:
        conditions.append(PartnerAccount.user_id != exclude_user_id)
    stmt = select(PartnerAccount.user_id).where(and_(*conditions))
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is None


async def set_custom_slug(session: AsyncSession, user_id: int, slug: str) -> PartnerAccount:
    model = await ensure_partner_account(session, user_id)
    model.custom_slug = _normalize_slug(slug)
    await session.flush()
    await session.refresh(model)
    return model


async def clear_custom_slug(session: AsyncSession, user_id: int) -> PartnerAccount:
    model = await ensure_partner_account(session, user_id)
    model.custom_slug = None
    await session.flush()
    await session.refresh(model)
    return model


async def set_partner_personal_percent(
    session: AsyncSession, user_id: int, percent: Optional[float]
) -> PartnerAccount:
    model = await ensure_partner_account(session, user_id)
    model.personal_percent = percent if percent is None else float(percent)
    await session.flush()
    await session.refresh(model)
    return model


async def set_partner_enabled(session: AsyncSession, user_id: int, enabled: bool) -> PartnerAccount:
    model = await ensure_partner_account(session, user_id)
    model.is_enabled = bool(enabled)
    await session.flush()
    await session.refresh(model)
    return model


async def get_referral_by_invited_user(
    session: AsyncSession, invited_user_id: int
) -> Optional[PartnerReferral]:
    stmt = select(PartnerReferral).where(PartnerReferral.invited_user_id == invited_user_id)
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def bind_referral_once(
    session: AsyncSession,
    *,
    partner_user_id: int,
    invited_user_id: int,
    linked_slug: Optional[str] = None,
) -> Tuple[bool, Optional[PartnerReferral]]:
    if partner_user_id == invited_user_id:
        return False, None

    existing = await get_referral_by_invited_user(session, invited_user_id)
    if existing:
        return False, existing

    model = PartnerReferral(
        partner_user_id=partner_user_id,
        invited_user_id=invited_user_id,
        linked_slug=_normalize_slug(linked_slug) if linked_slug else None,
    )
    session.add(model)
    await session.flush()
    await session.refresh(model)
    return True, model


async def get_partner_referral_counts(session: AsyncSession, partner_user_id: int) -> Dict[str, int]:
    invited_stmt = select(func.count(PartnerReferral.referral_id)).where(
        PartnerReferral.partner_user_id == partner_user_id
    )
    invited_count = int((await session.execute(invited_stmt)).scalar() or 0)

    paid_stmt = select(func.count(func.distinct(PartnerCommission.invited_user_id))).where(
        PartnerCommission.partner_user_id == partner_user_id
    )
    paid_count = int((await session.execute(paid_stmt)).scalar() or 0)
    return {"invited_count": invited_count, "paid_count": paid_count}


async def get_partner_money_stats(session: AsyncSession, partner_user_id: int) -> Dict[str, float]:
    turnover_stmt = select(func.coalesce(func.sum(PartnerCommission.payment_amount), 0.0)).where(
        PartnerCommission.partner_user_id == partner_user_id
    )
    turnover = float((await session.execute(turnover_stmt)).scalar() or 0.0)

    income_stmt = select(func.coalesce(func.sum(PartnerCommission.commission_amount), 0.0)).where(
        PartnerCommission.partner_user_id == partner_user_id
    )
    income = float((await session.execute(income_stmt)).scalar() or 0.0)
    return {"turnover": turnover, "income": income}


async def get_partner_referrals_with_income(
    session: AsyncSession,
    partner_user_id: int,
    *,
    limit: int = 30,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    paid_subq = (
        select(
            PartnerCommission.invited_user_id.label("invited_user_id"),
            func.coalesce(func.sum(PartnerCommission.payment_amount), 0.0).label("turnover"),
            func.coalesce(func.sum(PartnerCommission.commission_amount), 0.0).label("income"),
            func.count(PartnerCommission.commission_id).label("payments_count"),
        )
        .where(PartnerCommission.partner_user_id == partner_user_id)
        .group_by(PartnerCommission.invited_user_id)
        .subquery()
    )

    stmt = (
        select(
            PartnerReferral.invited_user_id,
            User.username,
            User.first_name,
            User.last_name,
            func.coalesce(paid_subq.c.turnover, 0.0),
            func.coalesce(paid_subq.c.income, 0.0),
            func.coalesce(paid_subq.c.payments_count, 0),
            PartnerReferral.linked_at,
        )
        .join(User, User.user_id == PartnerReferral.invited_user_id)
        .outerjoin(paid_subq, paid_subq.c.invited_user_id == PartnerReferral.invited_user_id)
        .where(PartnerReferral.partner_user_id == partner_user_id)
        .order_by(
            func.coalesce(paid_subq.c.income, 0.0).desc(),
            PartnerReferral.linked_at.desc(),
        )
        .offset(max(0, offset))
        .limit(limit)
    )
    result = await session.execute(stmt)
    rows = result.all()
    output: List[Dict[str, Any]] = []
    for row in rows:
        output.append(
            {
                "invited_user_id": int(row[0]),
                "username": row[1],
                "first_name": row[2],
                "last_name": row[3],
                "turnover": float(row[4] or 0.0),
                "income": float(row[5] or 0.0),
                "payments_count": int(row[6] or 0),
                "linked_at": row[7],
            }
        )
    return output


async def get_partner_referrals_total_count(
    session: AsyncSession, partner_user_id: int
) -> int:
    stmt = select(func.count(PartnerReferral.referral_id)).where(
        PartnerReferral.partner_user_id == partner_user_id
    )
    result = await session.execute(stmt)
    return int(result.scalar() or 0)


async def get_partner_commission_history(
    session: AsyncSession, partner_user_id: int, *, limit: int = 30
) -> List[PartnerCommission]:
    stmt = (
        select(PartnerCommission)
        .where(PartnerCommission.partner_user_id == partner_user_id)
        .order_by(PartnerCommission.created_at.desc(), PartnerCommission.commission_id.desc())
        .limit(limit)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def create_commission_if_absent(
    session: AsyncSession,
    *,
    partner_user_id: int,
    invited_user_id: int,
    payment_id: int,
    payment_amount: float,
    percent_applied: float,
    commission_amount: float,
    currency: str,
    sale_mode: Optional[str],
) -> Tuple[bool, Optional[PartnerCommission]]:
    existing_stmt = select(PartnerCommission).where(PartnerCommission.payment_id == payment_id)
    existing_result = await session.execute(existing_stmt)
    existing = existing_result.scalar_one_or_none()
    if existing:
        return False, existing

    model = PartnerCommission(
        partner_user_id=partner_user_id,
        invited_user_id=invited_user_id,
        payment_id=payment_id,
        payment_amount=float(payment_amount),
        percent_applied=float(percent_applied),
        commission_amount=float(commission_amount),
        currency=(currency or "RUB").upper(),
        sale_mode=sale_mode,
    )
    session.add(model)
    await session.flush()
    await session.refresh(model)
    return True, model


async def count_partners(session: AsyncSession, *, search: Optional[str] = None) -> int:
    stmt = select(func.count(PartnerAccount.user_id)).select_from(PartnerAccount).join(
        User, User.user_id == PartnerAccount.user_id
    )
    if search:
        value = search.strip().lower()
        conditions = [
            func.lower(func.coalesce(User.username, "")).like(f"%{value}%"),
            PartnerAccount.default_slug.like(f"%{value}%"),
            func.lower(func.coalesce(PartnerAccount.custom_slug, "")).like(f"%{value}%"),
        ]
        if value.isdigit():
            conditions.append(PartnerAccount.user_id == int(value))
        stmt = stmt.where(or_(*conditions))
    result = await session.execute(stmt)
    return int(result.scalar() or 0)


async def list_partners_paged(
    session: AsyncSession,
    *,
    page: int,
    page_size: int,
    search: Optional[str] = None,
) -> List[Tuple[PartnerAccount, User]]:
    safe_page = max(0, page)
    safe_size = max(1, page_size)

    stmt = (
        select(PartnerAccount, User)
        .join(User, User.user_id == PartnerAccount.user_id)
        .order_by(PartnerAccount.created_at.desc(), PartnerAccount.user_id.desc())
        .offset(safe_page * safe_size)
        .limit(safe_size)
    )
    if search:
        value = search.strip().lower()
        conditions = [
            func.lower(func.coalesce(User.username, "")).like(f"%{value}%"),
            PartnerAccount.default_slug.like(f"%{value}%"),
            func.lower(func.coalesce(PartnerAccount.custom_slug, "")).like(f"%{value}%"),
        ]
        if value.isdigit():
            conditions.append(PartnerAccount.user_id == int(value))
        stmt = stmt.where(or_(*conditions))

    result = await session.execute(stmt)
    return list(result.all())


async def ensure_accounts_for_existing_users(session: AsyncSession) -> int:
    users_stmt = select(User.user_id)
    users_result = await session.execute(users_stmt)
    user_ids = [int(uid) for uid in users_result.scalars().all()]
    created = 0
    for user_id in user_ids:
        existing = await get_partner_account(session, user_id)
        if existing:
            continue
        session.add(PartnerAccount(user_id=user_id, default_slug=build_default_slug(user_id)))
        created += 1
    if created:
        await session.flush()
    return created


async def get_payment_currency(session: AsyncSession, payment_id: int) -> str:
    stmt = select(Payment.currency).where(Payment.payment_id == payment_id)
    result = await session.execute(stmt)
    currency = result.scalar_one_or_none()
    return (currency or "RUB").upper()


async def get_partner_income_until_datetime(
    session: AsyncSession,
    partner_user_id: int,
    *,
    max_created_at: Optional[datetime] = None,
) -> float:
    conditions = [PartnerCommission.partner_user_id == partner_user_id]
    if max_created_at is not None:
        conditions.append(PartnerCommission.created_at <= max_created_at)

    stmt = select(func.coalesce(func.sum(PartnerCommission.commission_amount), 0.0)).where(
        and_(*conditions)
    )
    result = await session.execute(stmt)
    return float(result.scalar() or 0.0)


async def get_partner_withdrawal_sum_by_statuses(
    session: AsyncSession,
    user_id: int,
    statuses: Tuple[str, ...],
) -> float:
    if not statuses:
        return 0.0
    stmt = select(func.coalesce(func.sum(PartnerWithdrawalRequest.amount), 0.0)).where(
        PartnerWithdrawalRequest.user_id == user_id,
        PartnerWithdrawalRequest.status.in_(statuses),
    )
    result = await session.execute(stmt)
    return float(result.scalar() or 0.0)


async def get_partner_withdrawal_aggregates(session: AsyncSession, user_id: int) -> Dict[str, float]:
    reserved = await get_partner_withdrawal_sum_by_statuses(
        session, user_id, PARTNER_WITHDRAWAL_RESERVED_STATUSES
    )
    in_process = await get_partner_withdrawal_sum_by_statuses(
        session, user_id, PARTNER_WITHDRAWAL_IN_PROCESS_STATUSES
    )
    paid = await get_partner_withdrawal_sum_by_statuses(
        session, user_id, (PARTNER_WITHDRAWAL_STATUS_PAID,)
    )
    return {
        "reserved": reserved,
        "in_process": in_process,
        "paid": paid,
    }


async def get_active_partner_withdrawal_request_for_user(
    session: AsyncSession, user_id: int
) -> Optional[PartnerWithdrawalRequest]:
    stmt = (
        select(PartnerWithdrawalRequest)
        .where(
            PartnerWithdrawalRequest.user_id == user_id,
            PartnerWithdrawalRequest.status.in_(PARTNER_WITHDRAWAL_ACTIVE_STATUSES),
        )
        .order_by(
            PartnerWithdrawalRequest.created_at.desc(),
            PartnerWithdrawalRequest.request_id.desc(),
        )
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def create_partner_withdrawal_request(
    session: AsyncSession,
    *,
    user_id: int,
    amount: float,
    payout_method: str,
    payout_details: str,
    available_balance_snapshot: float,
    in_process_balance_snapshot: float,
    total_income_snapshot: float,
) -> PartnerWithdrawalRequest:
    model = PartnerWithdrawalRequest(
        user_id=user_id,
        amount=float(amount),
        payout_method=payout_method,
        payout_details=payout_details,
        status=PARTNER_WITHDRAWAL_STATUS_PENDING,
        available_balance_snapshot=float(available_balance_snapshot),
        in_process_balance_snapshot=float(in_process_balance_snapshot),
        total_income_snapshot=float(total_income_snapshot),
    )
    session.add(model)
    await session.flush()
    await session.refresh(model)
    return model


async def get_partner_withdrawal_request_by_id(
    session: AsyncSession, request_id: int
) -> Optional[PartnerWithdrawalRequest]:
    stmt = select(PartnerWithdrawalRequest).where(
        PartnerWithdrawalRequest.request_id == request_id
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def set_partner_withdrawal_admin_message_meta(
    session: AsyncSession,
    *,
    request_id: int,
    admin_chat_id: int,
    admin_thread_id: Optional[int],
    admin_message_id: int,
) -> Optional[PartnerWithdrawalRequest]:
    model = await get_partner_withdrawal_request_by_id(session, request_id)
    if not model:
        return None

    model.admin_chat_id = admin_chat_id
    model.admin_thread_id = admin_thread_id
    model.admin_message_id = admin_message_id
    await session.flush()
    await session.refresh(model)
    return model


async def approve_partner_withdrawal_request(
    session: AsyncSession,
    *,
    request_id: int,
    admin_id: int,
    admin_note: Optional[str] = None,
) -> Optional[PartnerWithdrawalRequest]:
    stmt = (
        update(PartnerWithdrawalRequest)
        .where(
            PartnerWithdrawalRequest.request_id == request_id,
            PartnerWithdrawalRequest.status == PARTNER_WITHDRAWAL_STATUS_PENDING,
        )
        .values(
            status=PARTNER_WITHDRAWAL_STATUS_APPROVED,
            processed_at=func.now(),
            processed_by_admin_id=admin_id,
            admin_note=admin_note,
            reject_reason=None,
        )
    )
    result = await session.execute(stmt)
    if not (result.rowcount or 0):
        return None
    return await get_partner_withdrawal_request_by_id(session, request_id)


async def reject_partner_withdrawal_request(
    session: AsyncSession,
    *,
    request_id: int,
    admin_id: int,
    reject_reason: Optional[str] = None,
) -> Optional[PartnerWithdrawalRequest]:
    stmt = (
        update(PartnerWithdrawalRequest)
        .where(
            PartnerWithdrawalRequest.request_id == request_id,
            PartnerWithdrawalRequest.status == PARTNER_WITHDRAWAL_STATUS_PENDING,
        )
        .values(
            status=PARTNER_WITHDRAWAL_STATUS_REJECTED,
            processed_at=func.now(),
            processed_by_admin_id=admin_id,
            reject_reason=reject_reason,
        )
    )
    result = await session.execute(stmt)
    if not (result.rowcount or 0):
        return None
    return await get_partner_withdrawal_request_by_id(session, request_id)


async def mark_partner_withdrawal_request_paid(
    session: AsyncSession,
    *,
    request_id: int,
    admin_id: int,
    admin_note: Optional[str] = None,
) -> Optional[PartnerWithdrawalRequest]:
    stmt = (
        update(PartnerWithdrawalRequest)
        .where(
            PartnerWithdrawalRequest.request_id == request_id,
            PartnerWithdrawalRequest.status == PARTNER_WITHDRAWAL_STATUS_APPROVED,
        )
        .values(
            status=PARTNER_WITHDRAWAL_STATUS_PAID,
            paid_at=func.now(),
            processed_at=func.coalesce(
                PartnerWithdrawalRequest.processed_at,
                func.now(),
            ),
            processed_by_admin_id=admin_id,
            admin_note=admin_note,
            reject_reason=None,
        )
    )
    result = await session.execute(stmt)
    if not (result.rowcount or 0):
        return None
    return await get_partner_withdrawal_request_by_id(session, request_id)

