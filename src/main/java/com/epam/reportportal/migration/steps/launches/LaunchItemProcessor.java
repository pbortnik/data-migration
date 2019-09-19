package com.epam.reportportal.migration.steps.launches;

import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Collections;

import static com.epam.reportportal.migration.steps.utils.MigrationUtils.SELECT_PROJECT_ID;
import static com.epam.reportportal.migration.steps.utils.MigrationUtils.SELECT_USER_ID;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class LaunchItemProcessor implements ItemProcessor<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Override
	public DBObject process(DBObject item) {
		try {
			Long userID = jdbcTemplate.queryForObject(SELECT_USER_ID, Collections.singletonMap("lg", item.get("userRef")), Long.class);
			item.put("userId", userID);
		} catch (Exception e) {
			LOGGER.info(String.format("User with name '%s' not found", item.get("userRef")));
		}
		try {
			Long projectId = jdbcTemplate.queryForObject(SELECT_PROJECT_ID,
					Collections.singletonMap("lg", item.get("projectRef")),
					Long.class
			);
			item.put("projectId", projectId);
		} catch (Exception e) {
			LOGGER.info(String.format("Project with name '%s' not found", item.get("projectRef")));
			return null;
		}
		return item;
	}
}
