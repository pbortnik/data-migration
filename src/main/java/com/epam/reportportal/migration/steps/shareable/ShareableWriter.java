package com.epam.reportportal.migration.steps.shareable;

import com.mongodb.DBObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class ShareableWriter {

	private static final String INSERT_SHAREABLE_ENTITY = "INSERT INTO shareable_entity (shared, owner, project_id) VALUES (?,?,?) RETURNING id";

	private static final String INSERT_OBJECT_IDENTITY =
			"INSERT INTO acl_object_identity (object_id_class, object_id_identity, owner_sid, entries_inheriting) "
					+ "VALUES (?, ?, ?, ?) RETURNING id";

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

	public void writeAcl(DBObject entity, Long entityId) {
		Long aclId = createAcl(entity, entityId);
		addPermissions(entity, entityId, aclId);
	}

	private void addPermissions(DBObject entity, Long entityId, Long aclId) {

	}

	private Long createAcl(DBObject entity, Long entityId) {
		return jdbcTemplate.queryForObject(INSERT_OBJECT_IDENTITY, Long.class, 1L, entityId, entity.get("ownerId"), Boolean.TRUE);
	}
}
