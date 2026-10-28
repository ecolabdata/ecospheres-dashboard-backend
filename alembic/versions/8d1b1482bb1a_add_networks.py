"""Add networks

Revision ID: 8d1b1482bb1a
Revises: 26e79fed6ea0
Create Date: 2026-10-12 18:51:57.814557

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8d1b1482bb1a"
down_revision: Union[str, None] = "26e79fed6ea0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "networks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("network_id", sa.String(), nullable=False, unique=True),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("deleted", sa.Boolean(), nullable=False),
    )
    op.create_table(
        "datasets_networks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("network_id", sa.String(), sa.ForeignKey("networks.network_id")),
        sa.Column("dataset_id", sa.String(), sa.ForeignKey("catalog.dataset_id")),
    )


def downgrade() -> None:
    op.drop_table("datasets_networks")
    op.drop_table("networks")
