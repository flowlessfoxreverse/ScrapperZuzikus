"""Initial schema — jobs and phone_numbers tables

Revision ID: 001
Revises: 
Create Date: 2024-01-01 00:00:00
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'jobs',
        sa.Column('id', UUID(as_uuid=False), primary_key=True),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('status', sa.Enum(
            'pending', 'processing', 'completed', 'failed', 'cancelled',
            name='jobstatus'
        ), nullable=False, server_default='pending'),
        sa.Column('total_numbers', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('processed_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('active_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('inactive_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('error_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('error_message', sa.Text(), nullable=True),
        sa.Column('meta', sa.JSON(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.Column('completed_at', sa.DateTime(), nullable=True),
    )
    op.create_index('ix_jobs_status', 'jobs', ['status'])

    op.create_table(
        'phone_numbers',
        sa.Column('id', UUID(as_uuid=False), primary_key=True),
        sa.Column('job_id', UUID(as_uuid=False),
                  sa.ForeignKey('jobs.id', ondelete='CASCADE'), nullable=False),
        sa.Column('phone', sa.String(30), nullable=False),
        sa.Column('phone_normalized', sa.String(30), nullable=True),
        sa.Column('status', sa.Enum(
            'pending', 'active', 'inactive', 'error',
            name='numberstatus'
        ), nullable=False, server_default='pending'),
        sa.Column('whatsapp_jid', sa.String(100), nullable=True),
        sa.Column('checked_at', sa.DateTime(), nullable=True),
        sa.Column('retry_count', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('error_message', sa.Text(), nullable=True),
    )
    op.create_index('ix_phone_numbers_job_id', 'phone_numbers', ['job_id'])
    op.create_index('ix_phone_numbers_phone', 'phone_numbers', ['phone'])
    op.create_index('ix_phone_numbers_status', 'phone_numbers', ['status'])


def downgrade() -> None:
    op.drop_table('phone_numbers')
    op.drop_table('jobs')
    op.execute('DROP TYPE IF EXISTS jobstatus')
    op.execute('DROP TYPE IF EXISTS numberstatus')
