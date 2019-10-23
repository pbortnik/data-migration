package com.epam.reportportal.migration.steps.items;

import com.epam.reportportal.migration.steps.CommonItemWriter;
import com.epam.reportportal.migration.steps.utils.CacheableDataService;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.Date;
import java.util.List;

import static com.epam.reportportal.migration.steps.items.TestProviderUtils.RETRY_SOURCE_PROVIDER;
import static com.epam.reportportal.migration.steps.items.TestProviderUtils.TEST_SOURCE_PROVIDER;
import static com.epam.reportportal.migration.steps.utils.MigrationUtils.toUtc;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@StepScope
@Component("testItemWriter")
public class TestItemWriter implements ItemWriter<DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final String INSERT_ITEM = "INSERT INTO test_item (uuid, name, type, start_time, description, last_modified,"
			+ "unique_id, has_children, has_retries, parent_id, launch_id, test_case_id) VALUES (:uid, :nm, :tp::TEST_ITEM_TYPE_ENUM,"
			+ ":st, :descr, :lm, :uq, :ch, :rtr, :par, :lid, :tci) ON CONFLICT DO NOTHING RETURNING item_id";

	private static final String INSERT_RETRY_ITEM = "INSERT INTO test_item (uuid, name, type, start_time, description, last_modified,"
			+ "unique_id, has_children, parent_id, retry_of) VALUES (:uid, :nm, :tp::TEST_ITEM_TYPE_ENUM,"
			+ ":st, :descr, :lm, :uq, :ch, :par, :rtrof) RETURNING item_id";

	private static final String UPDATE_PATH = "UPDATE test_item SET path = :path::LTREE WHERE item_id = :id";

	private static final String INSERT_ITEM_RESULTS = "INSERT INTO test_item_results (result_id, status, end_time, duration) VALUES "
			+ "(:id, :st::STATUS_ENUM, :ed, EXTRACT(EPOCH FROM (:ed::TIMESTAMP - :stime::TIMESTAMP)))";

	private static final String INSERT_ITEM_STATISTICS = "INSERT INTO statistics (s_counter, item_id, statistics_field_id) VALUES (:ct, :id, :sfi)";

	private static final String INSERT_ITEM_ATTRIBUTES = "INSERT INTO item_attribute (value, item_id) VALUES (:val, :id)";

	private static final String INSERT_ITEM_PARAMETERS = "INSERT INTO parameter (key, value, item_id) VALUES (:key, :val, :id)";

	private static final String INSERT_ISSUE =
			"INSERT INTO issue (issue_id, issue_type, issue_description, auto_analyzed, ignore_analyzer) "
					+ "VALUES (:id, :loc, :descr, :aa, :iga)";

	private static final String INSERT_TICKET_ISSUE = "INSERT INTO issue_ticket (issue_id, ticket_id) VALUES (:id, :tid)";

	@Autowired
	private JdbcTemplate jdbc;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private CommonItemWriter commonItemWriter;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Override
	public void write(List<? extends DBObject> items) {
		jdbc.execute("SET session_replication_role = REPLICA;");
		items.forEach(item -> {
			Long itemId = writeTestItem(item);
			if (itemId != null) {
				String path = (String) item.get("pathIds");
				item.put("pathIds", updatePath(path, itemId));
				writeItemResults(item, itemId);
				commonItemWriter.writeStatistics((DBObject) item.get("statistics"), INSERT_ITEM_STATISTICS, itemId);
				commonItemWriter.writeTags((BasicDBList) item.get("tags"), INSERT_ITEM_ATTRIBUTES, itemId);
				commonItemWriter.writeParams((BasicDBList) item.get("parameters"), INSERT_ITEM_PARAMETERS, itemId);

				if (item.get("issue") != null) {
					writeIssue((DBObject) item.get("issue"), itemId);
				}

				BasicDBList retries = (BasicDBList) item.get("retries");
				if (!CollectionUtils.isEmpty(retries)) {
					retries.forEach(retry -> writeRetry((DBObject) retry, item, itemId));
				}
			}
		});
	}

	private void writeRetry(DBObject retry, DBObject mainItem, Long mainItemId) {
		MapSqlParameterSource sqlParameterSource = (MapSqlParameterSource) RETRY_SOURCE_PROVIDER.createSqlParameterSource(retry);
		sqlParameterSource.addValue("par", mainItem.get("parentId"));
		sqlParameterSource.addValue("rtrof", mainItemId);

		Long retryId = jdbcTemplate.queryForObject(INSERT_RETRY_ITEM, sqlParameterSource, Long.class);

		updatePath((String) mainItem.get("pathIds"), retryId);

		writeItemResults(retry, retryId);
		commonItemWriter.writeTags((BasicDBList) retry.get("tags"), INSERT_ITEM_ATTRIBUTES, retryId);
		commonItemWriter.writeParams((BasicDBList) retry.get("parameters"), INSERT_ITEM_PARAMETERS, retryId);
	}

	private String updatePath(String path, Long itemId) {
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("id", itemId);
		String result = path;
		if (!StringUtils.isEmpty(path)) {
			result = path + "." + itemId;
			parameterSource.addValue("path", result);
		} else {
			result = String.valueOf(itemId);
			parameterSource.addValue("path", result);
		}
		jdbcTemplate.update(UPDATE_PATH, parameterSource);
		return result;
	}

	private Long writeTestItem(DBObject item) {
		try {
			return jdbcTemplate.queryForObject(INSERT_ITEM, TEST_SOURCE_PROVIDER.createSqlParameterSource(item), Long.class);
		} catch (DataAccessException e) {
			LOGGER.debug(String.format("Item '%s' already exists", item.get("_id").toString()));
			return null;
		}
	}

	private void writeItemResults(DBObject item, Long itemId) {
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("id", itemId);
		parameterSource.addValue("st", item.get("status"));
		parameterSource.addValue("stime", toUtc((Date) item.get("start_time")));
		parameterSource.addValue("ed", toUtc((Date) item.get("end_time")));
		jdbcTemplate.update(INSERT_ITEM_RESULTS, parameterSource);
	}

	private void writeIssue(DBObject issue, Long itemId) {
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("id", itemId);
		parameterSource.addValue("loc", issue.get("issueTypeId"));
		parameterSource.addValue("descr", issue.get("issueDescription"));
		parameterSource.addValue("aa", issue.get("autoAnalyzed"));
		parameterSource.addValue("iga", issue.get("ignoreAnalyzer"));
		jdbcTemplate.update(INSERT_ISSUE, parameterSource);

		BasicDBList tickets = (BasicDBList) issue.get("externalSystemIssues");
		if (!CollectionUtils.isEmpty(tickets)) {
			writeTickets(itemId, tickets);
		}
	}

	private void writeTickets(Long issueId, BasicDBList tickets) {
		tickets.forEach((ticket -> {
			Long ticketId = cacheableDataService.retrieveTicketId((DBObject) ticket);
			MapSqlParameterSource parameterSource = new MapSqlParameterSource();
			parameterSource.addValue("id", issueId);
			parameterSource.addValue("tid", ticketId);
			jdbcTemplate.update(INSERT_TICKET_ISSUE, parameterSource);
		}));
	}

}
