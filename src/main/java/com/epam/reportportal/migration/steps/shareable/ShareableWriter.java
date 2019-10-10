package com.epam.reportportal.migration.steps.shareable;

import com.google.common.collect.Lists;
import com.mongodb.DBObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class ShareableWriter {

	private static final String INSERT_SHAREABLE_ENTITY = "INSERT INTO shareable_entity (shared, owner, project_id) VALUES (?,?,?) RETURNING id";

	private static final String INSERT_OBJECT_IDENTITY =
			"INSERT INTO acl_object_identity (object_id_class, object_id_identity, owner_sid, entries_inheriting) "
					+ "VALUES (?, ?, ?, ?) RETURNING id";

	private static final String INSERT_ACL_ENTRY =
			"INSERT INTO acl_entry (acl_object_identity, ace_order, sid, mask, granting, audit_success, audit_failure)"
					+ "VALUES (?, ?, ?, ?, ?, ?, ?)";

	private static final String SELECT_PROJECT_USERS = "SELECT acl_sid.id, project_role, users.role FROM project_user "
			+ "JOIN users ON project_user.user_id = users.id JOIN acl_sid ON users.login = sid"
			+ " WHERE project_id = ? AND acl_sid.id != ?";

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public Long writeShareableEntity(DBObject item) {
		return jdbcTemplate.queryForObject(INSERT_SHAREABLE_ENTITY,
				Long.class,
				item.get("shared"),
				((DBObject) item.get("acl")).get("ownerUserId"),
				item.get("projectId")
		);
	}

	public void writeAcl(DBObject entity, Long entityId, Long aclClass) {
		Long aclObjectId = createAcl(entity, entityId, aclClass);
		if ((boolean) entity.get("shared")) {
			addPermissions(entity, aclObjectId);
		}
	}

	private void addPermissions(DBObject entity, Long aclObjectId) {
		Map<Long, String> users = getUsers(entity);

		if (users.isEmpty()) {
			return;
		}

		List<Object[]> params = new ArrayList<>();
		int i = 1;
		for (Map.Entry<Long, String> entry : users.entrySet()) {
			int mask = 1;
			if (entry.getValue().equalsIgnoreCase("PROJECT_MANAGER") || entry.getValue().equalsIgnoreCase("ADMINISTRATOR")) {
				mask = 16;
			}
			List<Object> sqlParams = Lists.newLinkedList();
			sqlParams.add(aclObjectId);
			sqlParams.add(i);
			sqlParams.add(entry.getKey());
			sqlParams.add(mask);
			sqlParams.add(true);
			sqlParams.add(false);
			sqlParams.add(false);
			params.add(sqlParams.toArray());
			i++;
		}

		jdbcTemplate.batchUpdate(INSERT_ACL_ENTRY, params);
	}

	private Map<Long, String> getUsers(DBObject entity) {
		return jdbcTemplate.query(SELECT_PROJECT_USERS, rs -> {
			Map<Long, String> users = new HashMap<>();
			while (rs.next()) {
				if (rs.getString(3).equalsIgnoreCase("ADMINISTRATOR")) {
					users.put(rs.getLong(1), rs.getString(3));
				} else {
					users.put(rs.getLong(1), rs.getString(2));
				}
			}
			return users;
		}, entity.get("projectId"), entity.get("ownerId"));
	}

	private Long createAcl(DBObject entity, Long entityId, Long aclClass) {
		Long objectId = jdbcTemplate.queryForObject(INSERT_OBJECT_IDENTITY,
				Long.class,
				aclClass,
				entityId,
				entity.get("ownerId"),
				Boolean.TRUE
		);
		jdbcTemplate.update(INSERT_ACL_ENTRY, objectId, 0, entity.get("ownerId"), 16, true, false, false);
		return objectId;
	}
}
