"""partner program rework: remove ads and legacy referral

Revision ID: 0004_partner_program_rework
Revises: 0003_promo_curr_act_not_null
Create Date: 2026-03-30 00:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0004_partner_program_rework"
down_revision: Union[str, Sequence[str], None] = "0003_promo_curr_act_not_null"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "partner_program_settings",
        sa.Column("id", sa.Integer(), autoincrement=False, nullable=False),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("default_percent", sa.Float(), nullable=False, server_default=sa.text("10")),
        sa.Column("allow_traffic_commission", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("min_payment_amount", sa.Float(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("id"),
    )

    op.create_table(
        "partner_accounts",
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("default_slug", sa.String(length=32), nullable=False),
        sa.Column("custom_slug", sa.String(length=32), nullable=True),
        sa.Column("personal_percent", sa.Float(), nullable=True),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"]),
        sa.PrimaryKeyConstraint("user_id"),
        sa.UniqueConstraint("default_slug"),
        sa.UniqueConstraint("custom_slug"),
    )
    op.create_index("ix_partner_accounts_default_slug", "partner_accounts", ["default_slug"], unique=False)
    op.create_index("ix_partner_accounts_custom_slug", "partner_accounts", ["custom_slug"], unique=False)
    op.create_index("ix_partner_accounts_is_enabled", "partner_accounts", ["is_enabled"], unique=False)

    op.create_table(
        "partner_referrals",
        sa.Column("referral_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("partner_user_id", sa.BigInteger(), nullable=False),
        sa.Column("invited_user_id", sa.BigInteger(), nullable=False),
        sa.Column("linked_slug", sa.String(length=32), nullable=True),
        sa.Column("linked_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.ForeignKeyConstraint(["invited_user_id"], ["users.user_id"]),
        sa.ForeignKeyConstraint(["partner_user_id"], ["users.user_id"]),
        sa.PrimaryKeyConstraint("referral_id"),
        sa.UniqueConstraint("invited_user_id"),
    )
    op.create_index("ix_partner_referrals_partner_user_id", "partner_referrals", ["partner_user_id"], unique=False)
    op.create_index("ix_partner_referrals_invited_user_id", "partner_referrals", ["invited_user_id"], unique=False)

    op.create_table(
        "partner_commissions",
        sa.Column("commission_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("partner_user_id", sa.BigInteger(), nullable=False),
        sa.Column("invited_user_id", sa.BigInteger(), nullable=False),
        sa.Column("payment_id", sa.Integer(), nullable=False),
        sa.Column("payment_amount", sa.Float(), nullable=False),
        sa.Column("percent_applied", sa.Float(), nullable=False),
        sa.Column("commission_amount", sa.Float(), nullable=False),
        sa.Column("currency", sa.String(), nullable=False, server_default=sa.text("'RUB'")),
        sa.Column("sale_mode", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.ForeignKeyConstraint(["invited_user_id"], ["users.user_id"]),
        sa.ForeignKeyConstraint(["partner_user_id"], ["users.user_id"]),
        sa.ForeignKeyConstraint(["payment_id"], ["payments.payment_id"]),
        sa.PrimaryKeyConstraint("commission_id"),
        sa.UniqueConstraint("payment_id"),
    )
    op.create_index("ix_partner_commissions_partner_user_id", "partner_commissions", ["partner_user_id"], unique=False)
    op.create_index("ix_partner_commissions_invited_user_id", "partner_commissions", ["invited_user_id"], unique=False)
    op.create_index("ix_partner_commissions_payment_id", "partner_commissions", ["payment_id"], unique=False)

    op.execute(
        sa.text(
            """
            INSERT INTO partner_program_settings (id, is_enabled, default_percent, allow_traffic_commission, min_payment_amount)
            VALUES (1, true, 10, false, 0)
            ON CONFLICT (id) DO NOTHING
            """
        )
    )

    op.execute(
        sa.text(
            """
            INSERT INTO partner_accounts (user_id, default_slug, is_enabled)
            SELECT u.user_id, lower('u' || u.user_id::text), true
            FROM users u
            ON CONFLICT (user_id) DO NOTHING
            """
        )
    )

    op.execute(
        sa.text(
            """
            INSERT INTO partner_referrals (partner_user_id, invited_user_id, linked_slug, linked_at)
            SELECT u.referred_by_id, u.user_id, NULL, NOW()
            FROM users u
            WHERE u.referred_by_id IS NOT NULL
              AND u.referred_by_id <> u.user_id
            ON CONFLICT (invited_user_id) DO NOTHING
            """
        )
    )

    op.execute("DROP INDEX IF EXISTS uq_users_referral_code")
    op.execute("DROP INDEX IF EXISTS ix_users_referral_code")
    op.execute("ALTER TABLE users DROP CONSTRAINT IF EXISTS users_referred_by_id_fkey")

    with op.batch_alter_table("users") as batch_op:
        batch_op.drop_column("referral_code")
        batch_op.drop_column("referred_by_id")

    op.execute("DROP INDEX IF EXISTS ix_ad_attributions_ad_campaign_id")
    op.execute("DROP TABLE IF EXISTS ad_attributions")
    op.execute("DROP INDEX IF EXISTS ix_ad_campaigns_is_active")
    op.execute("DROP INDEX IF EXISTS ix_ad_campaigns_source")
    op.execute("DROP TABLE IF EXISTS ad_campaigns")


def downgrade() -> None:
    op.create_table(
        "ad_campaigns",
        sa.Column("ad_campaign_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("start_param", sa.String(), nullable=False),
        sa.Column("cost", sa.Float(), nullable=False, server_default=sa.text("0")),
        sa.Column("is_active", sa.Boolean(), nullable=True, server_default=sa.text("true")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.PrimaryKeyConstraint("ad_campaign_id"),
        sa.UniqueConstraint("start_param"),
    )
    op.create_index("ix_ad_campaigns_source", "ad_campaigns", ["source"], unique=False)
    op.create_index("ix_ad_campaigns_is_active", "ad_campaigns", ["is_active"], unique=False)

    op.create_table(
        "ad_attributions",
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("ad_campaign_id", sa.Integer(), nullable=False),
        sa.Column("first_start_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("trial_activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["ad_campaign_id"], ["ad_campaigns.ad_campaign_id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.user_id"]),
        sa.PrimaryKeyConstraint("user_id"),
    )
    op.create_index("ix_ad_attributions_ad_campaign_id", "ad_attributions", ["ad_campaign_id"], unique=False)

    with op.batch_alter_table("users") as batch_op:
        batch_op.add_column(sa.Column("referral_code", sa.String(length=16), nullable=True))
        batch_op.add_column(sa.Column("referred_by_id", sa.BigInteger(), nullable=True))

    op.execute("ALTER TABLE users ADD CONSTRAINT users_referred_by_id_fkey FOREIGN KEY (referred_by_id) REFERENCES users (user_id)")
    op.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_users_referral_code ON users (referral_code) WHERE referral_code IS NOT NULL")

    op.drop_index("ix_partner_commissions_payment_id", table_name="partner_commissions")
    op.drop_index("ix_partner_commissions_invited_user_id", table_name="partner_commissions")
    op.drop_index("ix_partner_commissions_partner_user_id", table_name="partner_commissions")
    op.drop_table("partner_commissions")

    op.drop_index("ix_partner_referrals_invited_user_id", table_name="partner_referrals")
    op.drop_index("ix_partner_referrals_partner_user_id", table_name="partner_referrals")
    op.drop_table("partner_referrals")

    op.drop_index("ix_partner_accounts_is_enabled", table_name="partner_accounts")
    op.drop_index("ix_partner_accounts_custom_slug", table_name="partner_accounts")
    op.drop_index("ix_partner_accounts_default_slug", table_name="partner_accounts")
    op.drop_table("partner_accounts")

    op.drop_table("partner_program_settings")
