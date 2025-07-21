"""Add resource available

Revision ID: 9c87966e7c9e
Revises: 7d0e6fd2acf7
Create Date: 2025-07-21 12:19:21.854971

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9c87966e7c9e"
down_revision: Union[str, None] = "7d0e6fd2acf7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "resources", sa.Column("available", sa.Boolean(), nullable=False, server_default=sa.false())
    )


def downgrade() -> None:
    op.drop_column("resources", "available")
