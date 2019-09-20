package com.epam.reportportal.migration.steps.launches;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("launchItemWriter")
public class LaunchItemWriter implements ItemWriter<DBObject> {

	private static final String INSERT_LAUNCH =
			"INSERT INTO launch (uuid, project_id, user_id, name, description, start_time, end_time, number, last_modified,"
					+ "mode, status, approximate_duration) VALUES (gen_random_uuid(), :pr, :usr, :nm, :desc, :start, :end, :num, :last, "
					+ ":md::LAUNCH_MODE_ENUM, :st::STATUS_ENUM, :approx) RETURNING id";

	private static final String INSERT_LAUNCH_ATTRIBUTES = "INSERT INTO item_attribute (value, launch_id) VALUES (:val, :id)";


	private static final String TI_CUSTOM = "statistics$defects$to_investigate$%s";
	private static final String PB_CUSTOM = "statistics$defects$product_bug$%s";
	private static final String SI_CUSTOM = "statistics$defects$system_issue$%s";
	private static final String AB_CUSTOM = "statistics$defects$automation_bug$%s";
	private static final String ND_CUSTOM = "statistics$defects$no_defect$nd001";

	@Autowired
	@Qualifier("statisticsFields")
	private Map<String, Long> statisticsFields;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Override
	public void write(List<? extends DBObject> items) {
		items.forEach(it -> {
			Long id = jdbcTemplate.queryForObject(INSERT_LAUNCH,
					LaunchProviderUtils.LAUNCH_SOURCE_PROVIDER.createSqlParameterSource(it),
					Long.class
			);
			writeTags((BasicDBList) it.get("tags"), id);
			writeStatistics((DBObject) it.get("statistics"));
		});
	}

	private void writeStatistics(DBObject statistics) {

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
}
