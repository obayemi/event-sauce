-- Create events table for event store
CREATE TABLE IF NOT EXISTS events (
    -- Primary key
    id BIGSERIAL PRIMARY KEY,

    -- Event identification
    event_id UUID NOT NULL UNIQUE,
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,
    event_type VARCHAR(255) NOT NULL,
    event_version BIGINT NOT NULL,

    -- Event data
    event_data JSONB NOT NULL,

    -- Stream version (for optimistic concurrency)
    stream_version BIGINT NOT NULL,

    -- Audit fields
    created_by UUID,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Metadata
    correlation_id UUID,
    causation_id UUID,
    metadata JSONB,

    -- Constraints
    UNIQUE(aggregate_id, aggregate_type, stream_version)
);

-- Indexes for common queries
CREATE INDEX idx_events_aggregate ON events(aggregate_id, aggregate_type);
CREATE INDEX idx_events_aggregate_version ON events(aggregate_id, aggregate_type, stream_version);
CREATE INDEX idx_events_type ON events(event_type);
CREATE INDEX idx_events_aggregate_type ON events(aggregate_type);
CREATE INDEX idx_events_created_at ON events(created_at);
CREATE INDEX idx_events_correlation_id ON events(correlation_id) WHERE correlation_id IS NOT NULL;

-- Create snapshots table
CREATE TABLE IF NOT EXISTS snapshots (
    -- Primary key
    aggregate_id UUID NOT NULL,
    aggregate_type VARCHAR(255) NOT NULL,

    -- Snapshot data
    snapshot_version BIGINT NOT NULL,
    snapshot_data JSONB NOT NULL,

    -- Audit
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    -- Primary key constraint
    PRIMARY KEY (aggregate_id, aggregate_type)
);

-- Index for snapshot queries
CREATE INDEX idx_snapshots_type ON snapshots(aggregate_type);
