package com.epam.reportportal.migration.steps.launches;

import com.epam.reportportal.migration.steps.StatisticsFieldsService;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static com.epam.reportportal.migration.steps.StatisticsFieldsService.*;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("launchItemWriter")
public class LaunchItemWriter implements ItemWriter<DBObject> {

	private static final String INSERT_LAUNCH =
			"INSERT INTO launch (uuid, project_id, user_id, name, description, start_time, end_time, number, last_modified,"
					+ "mode, status, approximate_duration) VALUES (:uuid, :pr, :usr, :nm, :desc, :start, :end, :num, :last, "
					+ ":md::LAUNCH_MODE_ENUM, :st::STATUS_ENUM, :approx) RETURNING id;";

	private static final String INSERT_LAUNCH_ATTRIBUTES = "INSERT INTO item_attribute (value, launch_id) VALUES (:val, :id)";

	private static final String INSERT_LAUNCH_STATISTICS = "INSERT INTO statistics (s_counter, launch_id, statistics_field_id) VALUES (:ct, :lid, :sfi)";

	@Autowired
	private JdbcTemplate jdbc;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private StatisticsFieldsService fieldsService;

	@Override
	public void write(List<? extends DBObject> items) {
		items.forEach(it -> {
			jdbc.execute("SET session_replication_role = REPLICA;");
			Long id = jdbcTemplate.queryForObject(INSERT_LAUNCH,
					LaunchProviderUtils.LAUNCH_SOURCE_PROVIDER.createSqlParameterSource(it),
					Long.class
			);
			writeTags((BasicDBList) it.get("tags"), id);
			writeStatistics((DBObject) it.get("statistics"), id);
		});
	}

	private void writeTags(BasicDBList tags, Long id) {
		if (CollectionUtils.isEmpty(tags)) {
			return;
		}
		jdbcTemplate.batchUpdate(INSERT_LAUNCH_ATTRIBUTES, tags.stream().map(it -> {
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("val", it);
			params.addValue("id", id);
			return params;
		}).toArray(SqlParameterSource[]::new));
	}

	private void writeStatistics(DBObject statistics, Long launchId) {
		DBObject executions = (DBObject) statistics.get("executionCounter");

		SqlParameterSource[] executionParams = executions.keySet().stream().map(it -> {
			Integer counter = (Integer) executions.get(it);
			return populateValues(launchId, fieldsService.getPredefinedStatisticsFieldId(it), counter);
		}).filter(Objects::nonNull).toArray(SqlParameterSource[]::new);

		DBObject defects = (DBObject) statistics.get("issueCounter");

		SqlParameterSource[] params = Stream.of(executionParams,
				prepareParams("automationBug", AB_CUSTOM, launchId, defects),
				prepareParams("productBug", PB_CUSTOM, launchId, defects),
				prepareParams("systemIssue", SI_CUSTOM, launchId, defects),
				prepareParams("toInvestigate", TI_CUSTOM, launchId, defects),
				prepareParams("noDefect", ND_CUSTOM, launchId, defects)
		).flatMap(Arrays::stream).toArray(SqlParameterSource[]::new);

		jdbcTemplate.batchUpdate(INSERT_LAUNCH_STATISTICS, params);
	}

	private SqlParameterSource[] prepareParams(String type, String fieldRegex, Long launchId, DBObject defects) {
		DBObject bugType = (DBObject) defects.get(type);
		return bugType.keySet().stream().map(concreteField -> {
			Long defectFieldId = fieldsService.getStatisticsFieldId(fieldRegex, concreteField);
			Integer counter = (Integer) bugType.get(concreteField);
			return populateValues(launchId, defectFieldId, counter);
		}).filter(Objects::nonNull).toArray(SqlParameterSource[]::new);
	}

	private MapSqlParameterSource populateValues(Long launchId, Long fieldId, Integer counter) {
		if (counter > 0) {
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("ct", counter);
			params.addValue("lid", launchId);
			params.addValue("sfi", fieldId);
			return params;
		}
		return null;
	}
}
