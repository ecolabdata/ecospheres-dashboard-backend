"""Add DatasetMetric

Revision ID: eb04e2618b0f
Revises: 84c4d5a939e5
Create Date: 2025-06-23 07:29:11.844359

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "eb04e2618b0f"
down_revision: Union[str, None] = "84c4d5a939e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "datasets_metrics",
        sa.Column("dataset", sa.String(), nullable=True),
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("measurement", sa.String(), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("datasets_metrics")
