package com.epam.reportportal.migration.steps.projects;

import com.mongodb.DBObject;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("projectItemWriter")
@SuppressWarnings("unchecked")
public class ProjectItemWriter implements ItemWriter<DBObject> {

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Override
	public void write(List<? extends DBObject> items) {
		items.forEach(project -> {
			Long projectId = writeProject(project);
			writeProjectUsers(project, projectId);
		});
	}

	private Long writeProject(DBObject project) {
		Map<String, Object> params = new HashMap<>();
		params.put("nm", project.get("_id"));
		params.put("pt", ((DBObject) project.get("configuration")).get("entryType"));
		params.put("org", project.get("customer"));
		params.put("cd", project.get("creationDate"));
		params.put("md", "{\"metadata\": {\"migrated_from\": \"MongoDb\"}}");

		return jdbcTemplate.queryForObject(
				"INSERT INTO project (name, project_type, organization, creation_date, metadata) VALUES (:nm, :pt, :org, :cd, :md::JSONB) RETURNING project.id",
				params,
				Long.class
		);
	}

	private void writeProjectUsers(DBObject project, Long projectId) {
		Map<String, Object>[] users = ((List<Map<String, Object>>) project.get("users")).stream()
				.peek(it -> it.put("projectId", projectId))
				.toArray(Map[]::new);
		jdbcTemplate.batchUpdate(
				"INSERT INTO project_user (user_id, project_id, project_role) VALUES (:userId, :projectId, :projectRole::PROJECT_ROLE_ENUM)",
				users
		);
	}
}
