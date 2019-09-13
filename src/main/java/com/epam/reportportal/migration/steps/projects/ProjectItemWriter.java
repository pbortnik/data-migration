package com.epam.reportportal.migration.steps.projects;

import com.mongodb.DBObject;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.epam.reportportal.migration.steps.utils.ConverterUtils.toUtc;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("projectItemWriter")
@SuppressWarnings("unchecked")
public class ProjectItemWriter implements ItemWriter<DBObject> {

	private static final String INSERT_PROJECT = "INSERT INTO project (name, project_type, organization, creation_date, metadata) VALUES (:nm, :pt, :org, :cd, :md::JSONB) RETURNING project.id";

	private static final String INSERT_PROJECT_USER = "INSERT INTO project_user (user_id, project_id, project_role) VALUES (:userId, :projectId, :projectRole::PROJECT_ROLE_ENUM)";

	private static final String INSERT_ISSUE_TYPES = "INSERT INTO reportportal.public.issue_type (issue_group_id, locator, issue_name, abbreviation, hex_color) VALUES (:groupId, :locator, :longName, :shortName, :hexColor) RETURNING id";

	private static final String INSERT_PROJECT_ISSUE_TYPES = "INSERT INTO issue_type_project (project_id, issue_type_id) VALUES (:pr, :it)";

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	@Qualifier("issueGroups")
	private Map<String, Long> issueGroups;

	@Autowired
	@Qualifier("defaultIssueTypes")
	private Map<String, Long> defaultIssueTypes;

	@Override
	public void write(List<? extends DBObject> items) {
		items.forEach(project -> {
			Long projectId = writeProject(project);
			writeProjectUsers(project, projectId);
			writeProjectIssueTypes(project, projectId);
		});
	}

	private Long writeProject(DBObject project) {
		Map<String, Object> params = new HashMap<>();
		params.put("nm", project.get("_id"));
		params.put("pt", ((DBObject) project.get("configuration")).get("entryType"));
		params.put("org", project.get("customer"));
		params.put("cd", toUtc((Date) project.get("creationDate")));
		params.put("md", "{\"metadata\": {\"migrated_from\": \"MongoDb\"}}");

		return jdbcTemplate.queryForObject(INSERT_PROJECT, params, Long.class);
	}

	private void writeProjectUsers(DBObject project, Long projectId) {
		Map<String, Object>[] users = ((List<Map<String, Object>>) project.get("users")).stream()
				.peek(it -> it.put("projectId", projectId))
				.toArray(Map[]::new);
		jdbcTemplate.batchUpdate(INSERT_PROJECT_USER, users);
	}

	private List<Map> writeIssueTypes(Long projectId, DBObject subTypes, String issueGroup) {
		return ((List<Map>) subTypes.get(issueGroup)).stream().map(issueType -> {

			Map<String, Long> map = new HashMap<>();
			Long issueTypeId;

			String locator = ((String) issueType.get("locator")).toLowerCase();
			if (defaultIssueTypes.get(locator) != null) {
				issueTypeId = defaultIssueTypes.get(locator);
			} else {
				issueType.put("groupId", issueGroups.get(issueGroup));
				issueTypeId = jdbcTemplate.queryForObject(INSERT_ISSUE_TYPES, issueType, Long.class);
			}
			map.put("pr", projectId);
			map.put("it", issueTypeId);
			return map;
		}).collect(Collectors.toList());
	}

	private void writeProjectIssueTypes(DBObject project, Long projectId) {
		DBObject subTypes = (DBObject) ((DBObject) project.get("configuration")).get("subTypes");
		Map[] projectIssueTypesParams = issueGroups.keySet()
				.stream()
				.flatMap(issueGroup -> writeIssueTypes(projectId, subTypes, issueGroup).stream())
				.toArray(Map[]::new);
		jdbcTemplate.batchUpdate(INSERT_PROJECT_ISSUE_TYPES, projectIssueTypesParams);
	}
}
