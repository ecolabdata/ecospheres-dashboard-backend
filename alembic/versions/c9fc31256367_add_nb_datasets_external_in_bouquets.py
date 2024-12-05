"""add nb_datasets_external in bouquets

Revision ID: c9fc31256367
Revises: 20b0588a6afa
Create Date: 2024-12-05 11:46:39.713400

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c9fc31256367"
down_revision: Union[str, None] = "20b0588a6afa"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "bouquets",
        sa.Column("nb_datasets_external", sa.Integer(), server_default="0", nullable=False),
    )


def downgrade() -> None:
    op.drop_column("bouquets", "nb_datasets_external")
