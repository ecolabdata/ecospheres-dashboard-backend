"""Add dataset tags

Revision ID: 90f327cf1916
Revises: 728964a2daeb
Create Date: 2025-10-08 11:50:12.924853

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "90f327cf1916"
down_revision: Union[str, None] = "728964a2daeb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "catalog",
        sa.Column("tags", postgresql.ARRAY(sa.String()), nullable=False, server_default="{}"),
    )


def downgrade() -> None:
    op.drop_column("catalog", "tags")
