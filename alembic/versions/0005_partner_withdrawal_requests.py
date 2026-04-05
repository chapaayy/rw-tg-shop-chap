"""add partner withdrawal requests

Revision ID: 0005_partner_withdrawal_requests
Revises: 0004_partner_program_rework
Create Date: 2026-04-05 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0005_partner_withdrawal_requests"
down_revision: Union[str, Sequence[str], None] = "0004_partner_program_rework"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "partner_withdrawal_requests",
        sa.Column("request_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("amount", sa.Float(), nullable=False),
        sa.Column("payout_method", sa.String(length=32), nullable=False),
        sa.Column("payout_details", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False, server_default=sa.text("'pending'")),
        sa.Column(
            "available_balance_snapshot",
            sa.Float(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "in_process_balance_snapshot",
            sa.Float(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "total_income_snapshot",
            sa.Float(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("admin_note", sa.Text(), nullable=True),
        sa.Column("reject_reason", sa.Text(), nullable=True),
        sa.Column("admin_chat_id", sa.BigInteger(), nullable=True),
        sa.Column("admin_thread_id", sa.Integer(), nullable=True),
        sa.Column("admin_message_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("paid_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processed_by_admin_id", sa.BigInteger(), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"]),
        sa.PrimaryKeyConstraint("request_id"),
    )

    op.create_index(
        "ix_partner_withdrawal_requests_user_id",
        "partner_withdrawal_requests",
        ["user_id"],
        unique=False,
    )
    op.create_index(
        "ix_partner_withdrawal_requests_status",
        "partner_withdrawal_requests",
        ["status"],
        unique=False,
    )
    op.create_index(
        "ix_partner_withdrawal_requests_created_at",
        "partner_withdrawal_requests",
        ["created_at"],
        unique=False,
    )
    op.create_index(
        "uq_partner_withdrawal_requests_active_user",
        "partner_withdrawal_requests",
        ["user_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('pending', 'approved')"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_partner_withdrawal_requests_active_user",
        table_name="partner_withdrawal_requests",
    )
    op.drop_index(
        "ix_partner_withdrawal_requests_created_at",
        table_name="partner_withdrawal_requests",
    )
    op.drop_index(
        "ix_partner_withdrawal_requests_status",
        table_name="partner_withdrawal_requests",
    )
    op.drop_index(
        "ix_partner_withdrawal_requests_user_id",
        table_name="partner_withdrawal_requests",
    )
    op.drop_table("partner_withdrawal_requests")
