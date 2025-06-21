"""Add metrics to datasets

Revision ID: 6c3eb7bdc06c
Revises: 2ab72471ece0
Create Date: 2025-06-20 12:53:07.640035

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "6c3eb7bdc06c"
down_revision: Union[str, None] = "2ab72471ece0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("catalog", sa.Column("nb_visits_last_month", sa.Integer(), nullable=True))
    op.add_column(
        "catalog", sa.Column("nb_downloads_resources_last_month", sa.Integer(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("catalog", "nb_downloads_resources_last_month")
    op.drop_column("catalog", "nb_visits_last_month")
