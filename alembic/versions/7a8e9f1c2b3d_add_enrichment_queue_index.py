"""add enrichment queue index

Revision ID: 7a8e9f1c2b3d
Revises: eeb0761df8aa
Create Date: 2026-09-01 15:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '7a8e9f1c2b3d'
down_revision: Union[str, Sequence[str], None] = 'eeb0761df8aa'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index(
        'idx_users_enrichment_queue',
        'users',
        ['crawl_state', sa.text('discovery_score DESC'), 'id'],
        unique=False,
        postgresql_where=sa.text("crawl_state IN ('pending_enrichment', 'enrichment_failed')")
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        'idx_users_enrichment_queue',
        table_name='users',
        postgresql_where=sa.text("crawl_state IN ('pending_enrichment', 'enrichment_failed')")
    )
