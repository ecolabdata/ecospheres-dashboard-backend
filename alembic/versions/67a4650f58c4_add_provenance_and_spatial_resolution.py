"""Add provenance and spatial_resolution

Revision ID: 67a4650f58c4
Revises: 8d1b1482bb1a
Create Date: 2026-11-06 15:01:49.574881

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "67a4650f58c4"
down_revision: Union[str, None] = "8d1b1482bb1a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("catalog", sa.Column("provenance", sa.String(), nullable=True))
    op.add_column(
        "catalog",
        sa.Column("provenance__length__ok", sa.Boolean(), nullable=False, server_default="false"),
    )
    op.add_column(
        "catalog",
        sa.Column("provenance__length__bin", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "catalog",
        sa.Column("provenance__length__bin_label", sa.String(), nullable=False, server_default=""),
    )

    op.add_column("catalog", sa.Column("spatial_resolution", sa.String(), nullable=True))
    op.add_column(
        "catalog",
        sa.Column(
            "has_spatial_resolution", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
    )


def downgrade() -> None:
    op.drop_column("catalog", "provenance")
    op.drop_column("catalog", "provenance__length__ok")
    op.drop_column("catalog", "provenance__length__bin")
    op.drop_column("catalog", "provenance__length__bin_label")

    op.drop_column("catalog", "spatial_resolution")
    op.drop_column("catalog", "has_spatial_resolution")
