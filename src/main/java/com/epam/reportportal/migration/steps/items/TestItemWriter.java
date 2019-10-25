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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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

	private static final String INSERT_ITEM = "INSERT INTO test_item (item_id, uuid, name, type, start_time, description, last_modified,"
			+ "unique_id, has_children, has_retries, parent_id, launch_id, test_case_id) VALUES (:id, :uid, :nm, :tp::TEST_ITEM_TYPE_ENUM,"
			+ ":st, :descr, :lm, :uq, :ch, :rtr, :par, :lid, :tci) ON CONFLICT DO NOTHING RETURNING item_id";

	private static final String INSERT_RETRY_ITEM =
			"INSERT INTO test_item (item_id, uuid, name, type, start_time, description, last_modified,"
					+ "unique_id, has_children, parent_id, retry_of) VALUES (:id, :uid, :nm, :tp::TEST_ITEM_TYPE_ENUM,"
					+ ":st, :descr, :lm, :uq, :ch, :par, :rtrof) ON CONFLICT DO NOTHING";

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
		List<SqlParameterSource> testItemSrc = new ArrayList<>(items.size());
		List<SqlParameterSource> itemResultsSrc = new ArrayList<>(items.size());
		List<SqlParameterSource> testRetriesSrc = new ArrayList<>();
		List<SqlParameterSource> issuesSrc = new ArrayList<>();
		List<SqlParameterSource> issueTicketsSrc = new ArrayList<>();
		List<SqlParameterSource> statisticsSrc = new ArrayList<>(itemResultsSrc.size() * 4);
		List<SqlParameterSource> attributesSrc = new ArrayList<>(itemResultsSrc.size());
		List<SqlParameterSource> paramsSrc = new ArrayList<>(itemResultsSrc.size());

		final AtomicLong atomicCurrentId = new AtomicLong(jdbc.queryForObject("SELECT multi_nextval('test_item_item_id_seq', ?)",
				Long.class,
				items.size()
		));

		items.forEach(item -> {

			Long currentId = atomicCurrentId.getAndIncrement();

			testItemSrc.add(getTestItemParams(item, currentId));
			cacheableDataService.putMapping(item.get("_id").toString(), currentId);

			String path = (String) item.get("pathIds");
			item.put("pathIds", updatePath(path, currentId));

			itemResultsSrc.add(getItemResults(item, currentId));
			statisticsSrc.addAll(commonItemWriter.getStatisticsParams((DBObject) item.get("statistics"), currentId));
			attributesSrc.addAll(commonItemWriter.getAttributes((BasicDBList) item.get("tags"), currentId));
			paramsSrc.addAll(commonItemWriter.getParams((BasicDBList) item.get("parameters"), currentId));

			if (item.get("issue") != null) {
				issuesSrc.add(getIssue((DBObject) item.get("issue"), currentId, issueTicketsSrc));
			}

			BasicDBList retries = (BasicDBList) item.get("retries");
			if (!CollectionUtils.isEmpty(retries)) {
				updateSrcWithRetries(retries, item, currentId, testRetriesSrc, itemResultsSrc, attributesSrc, paramsSrc);
			}
		});
		jdbcTemplate.batchUpdate(INSERT_ITEM, testItemSrc.toArray(new SqlParameterSource[0]));
		jdbcTemplate.batchUpdate(INSERT_RETRY_ITEM, testRetriesSrc.toArray(new SqlParameterSource[0]));
		jdbcTemplate.batchUpdate(INSERT_ITEM_RESULTS, itemResultsSrc.toArray(new SqlParameterSource[0]));
		jdbcTemplate.batchUpdate(INSERT_ISSUE, issuesSrc.toArray(new SqlParameterSource[0]));
		jdbcTemplate.batchUpdate(INSERT_TICKET_ISSUE, issueTicketsSrc.toArray(new SqlParameterSource[0]));
		jdbcTemplate.batchUpdate(INSERT_ITEM_STATISTICS, statisticsSrc.toArray(new SqlParameterSource[0]));
		jdbcTemplate.batchUpdate(INSERT_ITEM_ATTRIBUTES, attributesSrc.toArray(new SqlParameterSource[0]));
		jdbcTemplate.batchUpdate(INSERT_ITEM_PARAMETERS, paramsSrc.toArray(new SqlParameterSource[0]));
	}

	private void updateSrcWithRetries(BasicDBList retries, DBObject mainItem, Long mainItemId, List<SqlParameterSource> retriesParams,
			List<SqlParameterSource> results, List<SqlParameterSource> tags, List<SqlParameterSource> params) {

		final AtomicLong currentRetryId = new AtomicLong(jdbc.queryForObject("SELECT multi_nextval('test_item_item_id_seq', ?)",
				Long.class,
				retries.size()
		));

		retries.stream().map(DBObject.class::cast).forEach(retry -> {
			Long currentId = currentRetryId.getAndIncrement();
			MapSqlParameterSource sqlParameterSource = (MapSqlParameterSource) RETRY_SOURCE_PROVIDER.createSqlParameterSource(retry);
			sqlParameterSource.addValue("id", currentId);
			sqlParameterSource.addValue("par", mainItem.get("parentId"));
			sqlParameterSource.addValue("rtrof", mainItemId);

			updatePath((String) mainItem.get("pathIds"), currentId);

			results.add(getItemResults(retry, currentId));
			tags.addAll(commonItemWriter.getAttributes((BasicDBList) retry.get("tags"), currentId));
			params.addAll(commonItemWriter.getParams((BasicDBList) retry.get("parameters"), currentId));
			retriesParams.add(sqlParameterSource);
		});
	}

	private String updatePath(String path, Long itemId) {
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("id", itemId);
		String result;
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

	private SqlParameterSource getTestItemParams(DBObject item, Long id) {
		MapSqlParameterSource sqlParameterSource = (MapSqlParameterSource) TEST_SOURCE_PROVIDER.createSqlParameterSource(item);
		sqlParameterSource.addValue("id", id);
		return sqlParameterSource;
	}

	private SqlParameterSource getItemResults(DBObject item, Long itemId) {
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("id", itemId);
		parameterSource.addValue("st", item.get("status"));
		parameterSource.addValue("stime", toUtc((Date) item.get("start_time")));
		parameterSource.addValue("ed", toUtc((Date) item.get("end_time")));
		return parameterSource;
	}

	private SqlParameterSource getIssue(DBObject issue, Long itemId, List<SqlParameterSource> issueTickets) {
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("id", itemId);
		parameterSource.addValue("loc", issue.get("issueTypeId"));
		parameterSource.addValue("descr", issue.get("issueDescription"));
		parameterSource.addValue("aa", issue.get("autoAnalyzed"));
		parameterSource.addValue("iga", issue.get("ignoreAnalyzer"));

		BasicDBList tickets = (BasicDBList) issue.get("externalSystemIssues");
		if (!CollectionUtils.isEmpty(tickets)) {
			issueTickets.addAll(getTicketParams(itemId, tickets));
		}
		return parameterSource;
	}

	private List<SqlParameterSource> getTicketParams(Long issueId, BasicDBList tickets) {
		return tickets.stream().map((ticket -> {
			Long ticketId = cacheableDataService.retrieveTicketId((DBObject) ticket);
			MapSqlParameterSource parameterSource = new MapSqlParameterSource();
			parameterSource.addValue("id", issueId);
			parameterSource.addValue("tid", ticketId);
			return parameterSource;
		})).collect(Collectors.toList());
	}

}
