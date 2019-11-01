-- launch
CREATE INDEX launch_project_idx
    ON launch (project_id);
CREATE INDEX launch_user_idx
    ON launch (user_id);

-- test item indexes
CREATE INDEX ti_parent_idx
    ON test_item (parent_id NULLS LAST);
CREATE INDEX ti_launch_idx
    ON test_item (launch_id NULLS LAST);
CREATE INDEX ti_retry_of_idx
    ON test_item (retry_of NULLS LAST);
CREATE INDEX test_item_unique_id_idx
    ON test_item (unique_id);
CREATE INDEX item_test_case_id_idx
    ON test_item (test_case_id);
CREATE INDEX test_item_unique_id_launch_id_idx
    ON test_item (unique_id, launch_id);
CREATE INDEX item_test_case_id_launch_id_idx
    ON test_item (test_case_id, launch_id);
CREATE INDEX path_gist_idx
    ON test_item
        USING gist (path);
CREATE INDEX path_idx
    ON test_item
        USING btree (path);

-- foreign keys test item
ALTER TABLE test_item
    ADD CONSTRAINT test_item_retry_of_fkey FOREIGN KEY (retry_of) REFERENCES test_item (item_id) ON DELETE CASCADE;

-- parameters
CREATE INDEX parameter_ti_idx
    ON parameter (item_id);

-- attributes
ALTER TABLE item_attribute
    ADD CONSTRAINT item_attribute_check CHECK ((item_id IS NOT NULL AND launch_id IS NULL) OR (item_id IS NULL AND launch_id IS NOT NULL));
CREATE INDEX item_attr_ti_idx
    ON item_attribute (item_id NULLS LAST);
CREATE INDEX item_attr_launch_idx
    ON item_attribute (launch_id NULLS LAST);

-- log
ALTER TABLE log
    ADD CONSTRAINT log_check CHECK ((item_id IS NOT NULL AND launch_id IS NULL) OR (item_id IS NULL AND launch_id IS NOT NULL)),
    ADD CONSTRAINT log_attachment_id_fkey FOREIGN KEY (attachment_id) REFERENCES attachment (id) ON DELETE SET NULL;

CREATE INDEX log_ti_idx
    ON log (item_id);
CREATE INDEX log_message_trgm_idx
    ON log USING gin (log_message gin_trgm_ops);
CREATE INDEX log_uuid_idx
    ON log USING hash (uuid);


-- statistics
ALTER TABLE statistics
    ADD CONSTRAINT statistics_statistics_field_id_fkey FOREIGN KEY (statistics_field_id) REFERENCES statistics_field (sf_id) ON DELETE CASCADE,
    ADD CONSTRAINT unique_stats_item UNIQUE (statistics_field_id, item_id),
    ADD CONSTRAINT unique_stats_launch UNIQUE (statistics_field_id, launch_id),
    ADD CONSTRAINT statistics_check
        CHECK (statistics.s_counter >= 0 AND ((item_id IS NOT NULL AND launch_id IS NULL) OR (launch_id IS NOT NULL AND item_id IS NULL)));

CREATE INDEX statistics_ti_idx
    ON statistics (item_id NULLS LAST);
CREATE INDEX statistics_launch_idx
    ON statistics (launch_id NULLS LAST);

-- issue
ALTER TABLE issue
    ADD CONSTRAINT issue_issue_id_fkey
        FOREIGN KEY (issue_id) REFERENCES test_item_results (result_id) ON DELETE CASCADE;
CREATE INDEX issue_it_idx
    ON issue (issue_type);

-- ticket
CREATE INDEX ticket_submitter_idx
    ON ticket (submitter);

--ticket_issue
ALTER TABLE reportportal.public.issue_ticket
    ADD CONSTRAINT issue_ticket_issue_id_fkey FOREIGN KEY (issue_id) REFERENCES issue (issue_id) ON DELETE CASCADE,
    ADD CONSTRAINT issue_ticket_ticket_id_fkey FOREIGN KEY (ticket_id) REFERENCES ticket (id);

-- autovacuum

ALTER TABLE test_item
    SET (AUTOVACUUM_ENABLED = TRUE),
    SET LOGGED;

ALTER TABLE test_item_results
    SET (AUTOVACUUM_ENABLED = TRUE),
    SET LOGGED;

ALTER TABLE item_attribute
    SET (AUTOVACUUM_ENABLED = TRUE),
    SET LOGGED;

ALTER TABLE parameter
    SET (AUTOVACUUM_ENABLED = TRUE),
    SET LOGGED;

ALTER TABLE issue
    SET (AUTOVACUUM_ENABLED = TRUE),
    SET LOGGED;

ALTER TABLE statistics
    SET (AUTOVACUUM_ENABLED = TRUE),
    SET LOGGED;

ALTER TABLE log
    SET (AUTOVACUUM_ENABLED = TRUE),
    SET LOGGED;

ANALYSE acl_class;
ANALYSE acl_entry;
ANALYSE acl_object_identity;
ANALYSE acl_sid;
ANALYSE attachment;
ANALYSE attribute;
ANALYSE content_field;
ANALYSE dashboard;
ANALYSE dashboard_widget;
ANALYSE filter;
ANALYSE filter_condition;
ANALYSE filter_sort;
ANALYSE integration;
ANALYSE integration_type;
ANALYSE issue;
ANALYSE issue_ticket;
ANALYSE issue_type;
ANALYSE issue_type_project;
ANALYSE item_attribute;
ANALYSE launch;
ANALYSE launch_attribute_rules;
ANALYSE launch_names;
ANALYSE launch_number;
ANALYSE log;
ANALYSE oauth_access_token;
ANALYSE parameter;
ANALYSE project;
ANALYSE project_attribute;
ANALYSE project_user;
ANALYSE recipients;
ANALYSE sender_case;
ANALYSE server_settings;
ANALYSE shareable_entity;
ANALYSE statistics;
ANALYSE statistics_field;
ANALYSE test_item;
ANALYSE test_item_results;
ANALYSE ticket;
ANALYSE user_preference;
ANALYSE users;
ANALYSE widget;
ANALYSE widget_filter;