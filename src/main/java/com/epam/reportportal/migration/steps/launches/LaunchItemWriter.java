package com.epam.reportportal.migration.steps.launches;

import com.epam.reportportal.migration.steps.CommonItemWriter;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("launchItemWriter")
@StepScope
public class LaunchItemWriter implements ItemWriter<DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final String INSERT_LAUNCH =
			"INSERT INTO launch (uuid, project_id, user_id, name, description, start_time, end_time, number, last_modified,"
					+ "mode, status, approximate_duration) VALUES (:uuid, :pr, :usr, :nm, :desc, :start, :end, :num, :last, "
					+ ":md::LAUNCH_MODE_ENUM, :st::STATUS_ENUM, :approx) ON CONFLICT DO NOTHING RETURNING id;";

	private static final String INSERT_LAUNCH_ATTRIBUTES = "INSERT INTO item_attribute (value, launch_id) VALUES (:val, :id)";

	private static final String INSERT_LAUNCH_STATISTICS = "INSERT INTO statistics (s_counter, launch_id, statistics_field_id) VALUES (:ct, :id, :sfi)";

	@Autowired
	private JdbcTemplate jdbc;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private CommonItemWriter commonItemWriter;

	@Override
	public void write(List<? extends DBObject> items) {
		items.forEach(it -> {
			jdbc.execute("SET session_replication_role = REPLICA;");
			try {
				Long id = jdbcTemplate.queryForObject(INSERT_LAUNCH,
						LaunchProviderUtils.LAUNCH_SOURCE_PROVIDER.createSqlParameterSource(it),
						Long.class
				);
				commonItemWriter.writeTags((BasicDBList) it.get("tags"), INSERT_LAUNCH_ATTRIBUTES, id);
				commonItemWriter.writeStatistics((DBObject) it.get("statistics"), INSERT_LAUNCH_STATISTICS, id);
			} catch (Exception e) {
				LOGGER.warn(String.format("Exception while inserting launch with uuid %s", ((ObjectId) it.get("_id")).toString()));
			}
		});
	}
}
