"""Add harvest issued_at

Revision ID: 728964a2daeb
Revises: 9c87966e7c9e
Create Date: 2025-10-03 15:17:14.880547

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "728964a2daeb"
down_revision: Union[str, None] = "9c87966e7c9e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("catalog", sa.Column("harvest__issued_at", sa.DateTime(), nullable=True))
    op.add_column("catalog", sa.Column("harvest__issued_at__year", sa.Integer(), nullable=True))
    op.add_column(
        "catalog",
        sa.Column(
            "has_harvest__issued_at", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
    )


def downgrade() -> None:
    op.drop_column("catalog", "has_harvest__issued_at")
    op.drop_column("catalog", "harvest__issued_at__year")
    op.drop_column("catalog", "harvest__issued_at")
