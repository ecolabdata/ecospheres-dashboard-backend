"""Add metrics to organization

Revision ID: 2ab72471ece0
Revises: 84c4d5a939e5
Create Date: 2025-06-20 12:36:19.516869

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2ab72471ece0"
down_revision: Union[str, None] = "84c4d5a939e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "organizations", sa.Column("nb_visits_datasets_last_month", sa.Integer(), nullable=True)
    )
    op.add_column(
        "organizations", sa.Column("nb_downloads_resources_last_month", sa.Integer(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("organizations", "nb_downloads_resources_last_month")
    op.drop_column("organizations", "nb_visits_datasets_last_month")
