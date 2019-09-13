package com.epam.reportportal.migration.steps.projects;

import com.mongodb.DBObject;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("projectItemProcessor")
@SuppressWarnings("unchecked")
public class ProjectItemProcessor implements ItemProcessor<DBObject, DBObject> {

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	private static final String SELECT_USER_ID = "SELECT id FROM users WHERE users.login = :lg";

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
						if (userID != null) {
							user.put("userId", userID);
						}
					} catch (Exception e) {
						System.out.println("no user with user name " + user.get("login"));
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
