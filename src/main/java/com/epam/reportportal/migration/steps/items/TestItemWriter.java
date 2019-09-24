package com.epam.reportportal.migration.steps.items;

import com.mongodb.DBObject;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("testItemWriter")
public class TestItemWriter implements ItemWriter<DBObject> {

	private static final String INSERT_ITEM = "INSERT INTO test_item (uuid, name, type, start_time, description, last_modified,"
			+ " path, unique_id, has_children, has_retries, parent_id, launch_id) VALUES (:uid, :nm, :tp::TEST_ITEM_TYPE_ENUM,"
			+ ":st, :descr, :lm, :path::LTREE, :uq, :ch, :rtr, :par, :lid)";

	private static final String INSERT_ITEM_RESULTS = "INSERT INTO test_item_results (result_id, status, end_time, duration) VALUES "
			+ "(:id, :st::STATUS_ENUM, :ed, EXTRACT(EPOCH FROM (:ed::TIMESTAMP - :stime::TIMESTAMP)))";

	private static final String INSERT_ITEM_STATISTICS = "INSERT INTO statistics (s_counter, item_id, statistics_field_id) VALUES (:ct, :lid, :sfi)";

	private static final String INSERT_ITEM_ATTRIBUTES = "INSERT INTO item_attribute (value, item_id) VALUES (:val, :id)";

	private static final String INSERT_ISSUE =
			"INSERT INTO issue (issue_id, issue_type, issue_description, auto_analyzed, ignore_analyzer) "
					+ "VALUES (:id, :it, :id, :aa, :iga)";

	private static final String INSERT_TICKET = "INSERT INTO ticket (ticket_id, submitter, submit_date, bts_url, bts_project, url) VALUES "
			+ "(:tid, :sub, :sd, :burl, :bpr, :url)";

	private static final String INSERT_TICKET_ISSUE = "INSERT INTO issue_ticket (issue_id, ticket_id) VALUES (:id, :tid)";

	@Override
	public void write(List<? extends DBObject> items) throws Exception {
		System.out.println(items);
	}
}
