-- =============================================================================
-- Output tables
-- =============================================================================

CREATE TABLE output_policy (
    policy_id BIGINT,
    policy_number TEXT,
    status TEXT,
    effective_date DATE,
    expiration_date DATE,
    holder_first_name TEXT,
    holder_last_name TEXT,
    holder_dob DATE,
    holder_email TEXT,
    holder_phone TEXT,
    holder_street TEXT,
    holder_city TEXT,
    holder_state TEXT,
    holder_zip TEXT,
    source_event_time TIMESTAMP,
    PRIMARY KEY (policy_id)
);

CREATE TABLE output_coverage (
    policy_id BIGINT,
    coverage_type TEXT,
    coverage_limit NUMERIC,
    deductible NUMERIC,
    premium NUMERIC,
    PRIMARY KEY (policy_id, coverage_type)
);

CREATE TABLE output_vehicle (
    policy_id BIGINT,
    vin TEXT,
    year_made INT,
    make TEXT,
    model TEXT,
    PRIMARY KEY (policy_id, vin)
);

CREATE TABLE output_driver (
    policy_id BIGINT,
    vehicle_vin TEXT,
    driver_name TEXT,
    license_number TEXT,
    is_primary BOOLEAN,
    PRIMARY KEY (policy_id, vehicle_vin, license_number)
);

CREATE TABLE output_claim (
    policy_id BIGINT,
    claim_id TEXT,
    claim_date DATE,
    amount NUMERIC,
    status TEXT,
    description TEXT,
    PRIMARY KEY (policy_id, claim_id)
);

-- =============================================================================
-- Staging tables (Spark writes here in append-only mode)
-- =============================================================================

CREATE TABLE stg_policy (
    stg_id BIGSERIAL,
    policy_id BIGINT,
    policy_number TEXT,
    status TEXT,
    effective_date DATE,
    expiration_date DATE,
    holder_first_name TEXT,
    holder_last_name TEXT,
    holder_dob DATE,
    holder_email TEXT,
    holder_phone TEXT,
    holder_street TEXT,
    holder_city TEXT,
    holder_state TEXT,
    holder_zip TEXT,
    source_event_time TIMESTAMP,
    op TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL
);

CREATE TABLE stg_coverage (
    stg_id BIGSERIAL,
    policy_id BIGINT,
    coverage_type TEXT,
    coverage_limit NUMERIC,
    deductible NUMERIC,
    premium NUMERIC,
    op TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL
);

CREATE TABLE stg_vehicle (
    stg_id BIGSERIAL,
    policy_id BIGINT,
    vin TEXT,
    year_made INT,
    make TEXT,
    model TEXT,
    op TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL
);

CREATE TABLE stg_driver (
    stg_id BIGSERIAL,
    policy_id BIGINT,
    vehicle_vin TEXT,
    driver_name TEXT,
    license_number TEXT,
    is_primary BOOLEAN,
    op TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL
);

CREATE TABLE stg_claim (
    stg_id BIGSERIAL,
    policy_id BIGINT,
    claim_id TEXT,
    claim_date DATE,
    amount NUMERIC,
    status TEXT,
    description TEXT,
    op TEXT NOT NULL,
    event_time TIMESTAMP NOT NULL
);

-- =============================================================================
-- Indexes on stg_id for merge performance
-- =============================================================================

CREATE INDEX idx_stg_policy_stg_id ON stg_policy(stg_id);
CREATE INDEX idx_stg_coverage_stg_id ON stg_coverage(stg_id);
CREATE INDEX idx_stg_vehicle_stg_id ON stg_vehicle(stg_id);
CREATE INDEX idx_stg_driver_stg_id ON stg_driver(stg_id);
CREATE INDEX idx_stg_claim_stg_id ON stg_claim(stg_id);

-- =============================================================================
-- Watermark table for tracking last-processed stg_id per entity
-- =============================================================================

CREATE TABLE merge_watermark (
    table_name TEXT PRIMARY KEY,
    last_stg_id BIGINT NOT NULL DEFAULT 0
);

INSERT INTO merge_watermark (table_name) VALUES
    ('stg_policy'), ('stg_coverage'), ('stg_vehicle'), ('stg_driver'), ('stg_claim');

-- =============================================================================
-- merge_cdc_batch(): merges all 5 entity types in a single transaction
-- =============================================================================

CREATE OR REPLACE FUNCTION merge_cdc_batch()
RETURNS JSON AS $$
DECLARE
    v_start TIMESTAMPTZ := clock_timestamp();
    v_result JSON;
    v_policy_count INT := 0;
    v_coverage_count INT := 0;
    v_vehicle_count INT := 0;
    v_driver_count INT := 0;
    v_claim_count INT := 0;
    v_policy_wm BIGINT;
    v_coverage_wm BIGINT;
    v_vehicle_wm BIGINT;
    v_driver_wm BIGINT;
    v_claim_wm BIGINT;
BEGIN
    -- === POLICY ===
    -- Step 1: Delete affected keys
    DELETE FROM output_policy
    WHERE policy_id IN (
        SELECT DISTINCT policy_id FROM stg_policy
        WHERE stg_id > (SELECT last_stg_id FROM merge_watermark WHERE table_name = 'stg_policy')
    );
    -- Step 2: Insert latest non-delete rows
    WITH new_batch AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY policy_id ORDER BY event_time DESC, stg_id DESC
        ) AS rn
        FROM stg_policy
        WHERE stg_id > (SELECT last_stg_id FROM merge_watermark WHERE table_name = 'stg_policy')
    )
    INSERT INTO output_policy (policy_id, policy_number, status, effective_date, expiration_date,
        holder_first_name, holder_last_name, holder_dob, holder_email, holder_phone,
        holder_street, holder_city, holder_state, holder_zip, source_event_time)
    SELECT policy_id, policy_number, status, effective_date, expiration_date,
        holder_first_name, holder_last_name, holder_dob, holder_email, holder_phone,
        holder_street, holder_city, holder_state, holder_zip, source_event_time
    FROM new_batch WHERE rn = 1 AND op != 'd';
    GET DIAGNOSTICS v_policy_count = ROW_COUNT;
    -- Step 3: Advance watermark
    UPDATE merge_watermark SET last_stg_id = COALESCE(
        (SELECT MAX(stg_id) FROM stg_policy WHERE stg_id > last_stg_id), last_stg_id)
    WHERE table_name = 'stg_policy'
    RETURNING last_stg_id INTO v_policy_wm;

    -- === COVERAGE ===
    DELETE FROM output_coverage
    WHERE (policy_id, coverage_type) IN (
        SELECT DISTINCT policy_id, coverage_type FROM stg_coverage
        WHERE stg_id > (SELECT last_stg_id FROM merge_watermark WHERE table_name = 'stg_coverage')
    );
    WITH new_batch AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY policy_id, coverage_type ORDER BY event_time DESC, stg_id DESC
        ) AS rn
        FROM stg_coverage
        WHERE stg_id > (SELECT last_stg_id FROM merge_watermark WHERE table_name = 'stg_coverage')
    )
    INSERT INTO output_coverage (policy_id, coverage_type, coverage_limit, deductible, premium)
    SELECT policy_id, coverage_type, coverage_limit, deductible, premium
    FROM new_batch WHERE rn = 1 AND op != 'd';
    GET DIAGNOSTICS v_coverage_count = ROW_COUNT;
    UPDATE merge_watermark SET last_stg_id = COALESCE(
        (SELECT MAX(stg_id) FROM stg_coverage WHERE stg_id > last_stg_id), last_stg_id)
    WHERE table_name = 'stg_coverage'
    RETURNING last_stg_id INTO v_coverage_wm;

    -- === VEHICLE ===
    DELETE FROM output_vehicle
    WHERE (policy_id, vin) IN (
        SELECT DISTINCT policy_id, vin FROM stg_vehicle
        WHERE stg_id > (SELECT last_stg_id FROM merge_watermark WHERE table_name = 'stg_vehicle')
    );
    WITH new_batch AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY policy_id, vin ORDER BY event_time DESC, stg_id DESC
        ) AS rn
        FROM stg_vehicle
        WHERE stg_id > (SELECT last_stg_id FROM merge_watermark WHERE table_name = 'stg_vehicle')
    )
    INSERT INTO output_vehicle (policy_id, vin, year_made, make, model)
    SELECT policy_id, vin, year_made, make, model
    FROM new_batch WHERE rn = 1 AND op != 'd';
    GET DIAGNOSTICS v_vehicle_count = ROW_COUNT;
    UPDATE merge_watermark SET last_stg_id = COALESCE(
        (SELECT MAX(stg_id) FROM stg_vehicle WHERE stg_id > last_stg_id), last_stg_id)
    WHERE table_name = 'stg_vehicle'
    RETURNING last_stg_id INTO v_vehicle_wm;

    -- === DRIVER ===
    DELETE FROM output_driver
    WHERE (policy_id, vehicle_vin, license_number) IN (
        SELECT DISTINCT policy_id, vehicle_vin, license_number FROM stg_driver
        WHERE stg_id > (SELECT last_stg_id FROM merge_watermark WHERE table_name = 'stg_driver')
    );
    WITH new_batch AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY policy_id, vehicle_vin, license_number ORDER BY event_time DESC, stg_id DESC
        ) AS rn
        FROM stg_driver
        WHERE stg_id > (SELECT last_stg_id FROM merge_watermark WHERE table_name = 'stg_driver')
    )
    INSERT INTO output_driver (policy_id, vehicle_vin, driver_name, license_number, is_primary)
    SELECT policy_id, vehicle_vin, driver_name, license_number, is_primary
    FROM new_batch WHERE rn = 1 AND op != 'd';
    GET DIAGNOSTICS v_driver_count = ROW_COUNT;
    UPDATE merge_watermark SET last_stg_id = COALESCE(
        (SELECT MAX(stg_id) FROM stg_driver WHERE stg_id > last_stg_id), last_stg_id)
    WHERE table_name = 'stg_driver'
    RETURNING last_stg_id INTO v_driver_wm;

    -- === CLAIM ===
    DELETE FROM output_claim
    WHERE (policy_id, claim_id) IN (
        SELECT DISTINCT policy_id, claim_id FROM stg_claim
        WHERE stg_id > (SELECT last_stg_id FROM merge_watermark WHERE table_name = 'stg_claim')
    );
    WITH new_batch AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY policy_id, claim_id ORDER BY event_time DESC, stg_id DESC
        ) AS rn
        FROM stg_claim
        WHERE stg_id > (SELECT last_stg_id FROM merge_watermark WHERE table_name = 'stg_claim')
    )
    INSERT INTO output_claim (policy_id, claim_id, claim_date, amount, status, description)
    SELECT policy_id, claim_id, claim_date, amount, status, description
    FROM new_batch WHERE rn = 1 AND op != 'd';
    GET DIAGNOSTICS v_claim_count = ROW_COUNT;
    UPDATE merge_watermark SET last_stg_id = COALESCE(
        (SELECT MAX(stg_id) FROM stg_claim WHERE stg_id > last_stg_id), last_stg_id)
    WHERE table_name = 'stg_claim'
    RETURNING last_stg_id INTO v_claim_wm;

    -- Build JSON result
    SELECT json_build_object(
        'elapsed_ms', ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start)) * 1000),
        'policy', json_build_object('rows_merged', v_policy_count, 'watermark', v_policy_wm),
        'coverage', json_build_object('rows_merged', v_coverage_count, 'watermark', v_coverage_wm),
        'vehicle', json_build_object('rows_merged', v_vehicle_count, 'watermark', v_vehicle_wm),
        'driver', json_build_object('rows_merged', v_driver_count, 'watermark', v_driver_wm),
        'claim', json_build_object('rows_merged', v_claim_count, 'watermark', v_claim_wm)
    ) INTO v_result;

    RETURN v_result;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- NOTIFY triggers on staging tables
-- =============================================================================

CREATE OR REPLACE FUNCTION notify_stg_data_arrived()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('stg_data_arrived', TG_TABLE_NAME);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER stg_policy_notify AFTER INSERT ON stg_policy
    FOR EACH STATEMENT EXECUTE FUNCTION notify_stg_data_arrived();
CREATE TRIGGER stg_coverage_notify AFTER INSERT ON stg_coverage
    FOR EACH STATEMENT EXECUTE FUNCTION notify_stg_data_arrived();
CREATE TRIGGER stg_vehicle_notify AFTER INSERT ON stg_vehicle
    FOR EACH STATEMENT EXECUTE FUNCTION notify_stg_data_arrived();
CREATE TRIGGER stg_driver_notify AFTER INSERT ON stg_driver
    FOR EACH STATEMENT EXECUTE FUNCTION notify_stg_data_arrived();
CREATE TRIGGER stg_claim_notify AFTER INSERT ON stg_claim
    FOR EACH STATEMENT EXECUTE FUNCTION notify_stg_data_arrived();
