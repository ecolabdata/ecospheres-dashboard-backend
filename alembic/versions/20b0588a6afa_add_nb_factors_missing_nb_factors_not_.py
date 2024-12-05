"""add nb_factors_missing, nb_factors_not_available in bouquets

Revision ID: 20b0588a6afa
Revises: af780a5cffbe
Create Date: 2024-12-05 11:34:33.692190

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "20b0588a6afa"
down_revision: Union[str, None] = "af780a5cffbe"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "bouquets",
        sa.Column("nb_factors_missing", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "bouquets",
        sa.Column("nb_factors_not_available", sa.Integer(), server_default="0", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("bouquets", "nb_factors_not_available")
    op.drop_column("bouquets", "nb_factors_missing")
