package com.epam.reportportal.migration.steps.projects;

import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.epam.reportportal.migration.steps.utils.MigrationUtils.SELECT_USER_ID;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("projectItemProcessor")
@SuppressWarnings("unchecked")
public class ProjectItemProcessor implements ItemProcessor<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Override
	public DBObject process(DBObject project) {
		return extractUserIds(project);
	}

	private DBObject extractUserIds(DBObject project) {
		List<DBObject> users = (List<DBObject>) project.get("users");

		if (CollectionUtils.isEmpty(users)) {
			return null;
		}

		List<DBObject> filtered = users.stream()
				.peek(user -> {
					try {
						Long userID = jdbcTemplate.queryForObject(SELECT_USER_ID,
								Collections.singletonMap("lg", (String) user.get("login")),
								Long.class
						);
						user.put("userId", userID);
					} catch (Exception e) {
						LOGGER.info(String.format("User with name '%s' not found", user.get("login")));
					}
				})
				.filter(it -> it.get("userId") != null)
				.collect(Collectors.groupingBy(it -> it.get("userId")))
				.values()
				.stream()
				.map(it -> it.stream().findFirst().get())
				.collect(Collectors.toList());

		project.put("users", filtered);
		return project;
	}
}
