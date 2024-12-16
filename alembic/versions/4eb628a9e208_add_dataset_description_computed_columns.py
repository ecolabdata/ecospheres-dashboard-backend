"""add Dataset.description computed columns

Revision ID: 4eb628a9e208
Revises: b99ee027d14f
Create Date: 2024-12-16 10:07:24.905942

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "4eb628a9e208"
down_revision: Union[str, None] = "b99ee027d14f"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "catalog",
        sa.Column("description__length", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "catalog",
        sa.Column("description__length__ok", sa.Boolean(), nullable=False, server_default="false"),
    )


def downgrade() -> None:
    op.drop_column("catalog", "description__length__ok")
    op.drop_column("catalog", "description__length")
