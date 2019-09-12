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
public class ProjectItemWriter implements ItemWriter<DBObject> {

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Override
	public void write(List<? extends DBObject> items) {
		items.forEach(project -> {
			Long id = writeProject(project);
			System.out.println(id);
		});
	}

	private Long writeProject(DBObject project) {
		Map<String, Object> params = new HashMap<>();
		params.put("nm", project.get("_id"));
		params.put("pt", ((DBObject) project.get("configuration")).get("entryType"));
		params.put("org", project.get("customer"));
		params.put("cd", project.get("creationDate"));

		return jdbcTemplate.queryForObject(
				"INSERT INTO project (name, project_type, organization, creation_date) VALUES (:nm, :pt, :org, :cd ) RETURNING project.id",
				params,
				Long.class
		);
	}
}
