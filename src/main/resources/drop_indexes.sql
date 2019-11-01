CREATE OR REPLACE FUNCTION multi_nextval(use_seqname REGCLASS,
                                         use_increment INTEGER) RETURNS BIGINT AS
$$
DECLARE
    reply   BIGINT;
    lock_id BIGINT := (use_seqname::BIGINT - 2147483648)::INTEGER;
BEGIN
    PERFORM pg_advisory_lock(lock_id);
    reply := nextval(use_seqname);
    PERFORM setval(use_seqname, reply + use_increment - 1, TRUE);
    PERFORM pg_advisory_unlock(lock_id);
    RETURN reply;
END;
$$ LANGUAGE plpgsql;


-- launch
DROP INDEX launch_project_idx;
DROP INDEX launch_user_idx;

-- test item indexes
DROP INDEX ti_parent_idx;
DROP INDEX ti_launch_idx;
DROP INDEX ti_retry_of_idx;
DROP INDEX test_item_unique_id_idx;
DROP INDEX item_test_case_id_idx;
DROP INDEX test_item_unique_id_launch_id_idx;
DROP INDEX item_test_case_id_launch_id_idx;
DROP INDEX path_gist_idx;
DROP INDEX path_idx;

-- foreign keys test item
ALTER TABLE test_item
    DROP CONSTRAINT test_item_retry_of_fkey;

-- parameters
DROP INDEX parameter_ti_idx;

-- attributes
ALTER TABLE item_attribute
    DROP CONSTRAINT item_attribute_check;
DROP INDEX item_attr_ti_idx;
DROP INDEX item_attr_launch_idx;

-- log
ALTER TABLE log
    DROP CONSTRAINT log_check,
    DROP CONSTRAINT log_attachment_id_fkey,
    DROP CONSTRAINT log_uuid_key;

DROP INDEX log_ti_idx;
DROP INDEX log_message_trgm_idx;
DROP INDEX log_uuid_idx;


-- statistics
ALTER TABLE statistics
    DROP CONSTRAINT statistics_statistics_field_id_fkey,
    DROP CONSTRAINT unique_stats_item,
    DROP CONSTRAINT unique_stats_launch,
    DROP CONSTRAINT statistics_check;

DROP INDEX statistics_ti_idx;
DROP INDEX statistics_launch_idx;

-- issue
ALTER TABLE issue
    DROP CONSTRAINT issue_issue_id_fkey;

DROP INDEX issue_it_idx;

-- ticket
DROP INDEX ticket_submitter_idx;

--ticket_issue
ALTER TABLE reportportal.public.issue_ticket
    DROP CONSTRAINT issue_ticket_issue_id_fkey,
    DROP CONSTRAINT issue_ticket_ticket_id_fkey;

-- autovacuum

ALTER TABLE test_item
    SET (AUTOVACUUM_ENABLED = FALSE),
    SET UNLOGGED;

ALTER TABLE test_item_results
    SET (AUTOVACUUM_ENABLED = FALSE),
    SET UNLOGGED;

ALTER TABLE item_attribute
    SET (AUTOVACUUM_ENABLED = FALSE),
    SET UNLOGGED;

ALTER TABLE parameter
    SET (AUTOVACUUM_ENABLED = FALSE),
    SET UNLOGGED;

ALTER TABLE issue
    SET (AUTOVACUUM_ENABLED = FALSE),
    SET UNLOGGED;

ALTER TABLE statistics
    SET (AUTOVACUUM_ENABLED = FALSE),
    SET UNLOGGED;

ALTER TABLE log
    SET (AUTOVACUUM_ENABLED = FALSE),
    SET UNLOGGED;