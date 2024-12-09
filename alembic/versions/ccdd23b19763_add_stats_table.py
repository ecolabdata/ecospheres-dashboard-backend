"""add stats table

Revision ID: ccdd23b19763
Revises: af780a5cffbe
Create Date: 2024-12-06 09:10:27.348826

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ccdd23b19763"
# FIXME: check that https://github.com/ecolabdata/ecospheres-dashboard-backend/pull/29 has not been merged first
down_revision: Union[str, None] = "af780a5cffbe"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "stats",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("nb_uniq_visitors", sa.Integer(), nullable=False),
        sa.Column("nb_visits", sa.Integer(), nullable=False),
        sa.Column("nb_actions", sa.Integer(), nullable=False),
        sa.Column("nb_visits_converted", sa.Integer(), nullable=False),
        sa.Column("bounce_count", sa.Integer(), nullable=False),
        sa.Column("sum_visit_length", sa.Integer(), nullable=False),
        sa.Column("max_actions", sa.Integer(), nullable=False),
        sa.Column("bounce_rate", sa.Float(), nullable=False),
        sa.Column("nb_actions_per_visit", sa.Float(), nullable=False),
        sa.Column("avg_time_on_site", sa.Integer(), nullable=False),
        sa.Column("nb_pageviews", sa.Integer(), nullable=False),
        sa.Column("nb_downloads", sa.Integer(), nullable=False),
        sa.Column("nb_uniq_visitors_returning", sa.Integer(), nullable=False),
        sa.Column("nb_uniq_visitors_new", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("stats")
