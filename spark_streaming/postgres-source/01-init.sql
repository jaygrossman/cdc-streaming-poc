-- Source table
CREATE TABLE policy (
    id BIGSERIAL PRIMARY KEY,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Auto-update the updated_at timestamp on changes
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER policy_updated_at
    BEFORE UPDATE ON policy
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

-- Enable full replica identity so Debezium captures before-image on deletes
ALTER TABLE policy REPLICA IDENTITY FULL;

-- Explicitly create the publication for Debezium
CREATE PUBLICATION policy_pub FOR TABLE policy;
