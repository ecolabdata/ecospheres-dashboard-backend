"""Add Stats.period column

Revision ID: a1b2c3d4e5f6
Revises: e80f9232f15c
Create Date: 2026-04-17 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "e80f9232f15c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """
    Add Stats.period column

    :return: None
    """
    op.add_column("stats", sa.Column("period", sa.String(), nullable=False, server_default="day"))


def downgrade() -> None:
    """
    Drop Stats.period column

    :return: None
    """
    op.drop_column("stats", "period")
