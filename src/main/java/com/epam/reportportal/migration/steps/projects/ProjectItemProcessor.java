package com.epam.reportportal.migration.steps.projects;

import com.mongodb.DBObject;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("projectItemProcessor")
public class ProjectItemProcessor implements ItemProcessor<DBObject, DBObject> {

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	private static final String SELECT_USER_ID = "SELECT id FROM users WHERE users.login = :lg";

	@Override

	public DBObject process(DBObject item) throws Exception {

		extractUserIds(item);
		return item;
	}

	@SuppressWarnings("unchecked")
	private void extractUserIds(DBObject item) {
		List<DBObject> users = (List<DBObject>) item.get("users");
		if (!CollectionUtils.isEmpty(users)) {
			users.forEach(user -> {
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
			});
		}
	}
}
