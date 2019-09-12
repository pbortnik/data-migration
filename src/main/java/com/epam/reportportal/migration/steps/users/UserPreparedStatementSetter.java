package com.epam.reportportal.migration.steps.users;

import com.mongodb.DBObject;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public class UserPreparedStatementSetter implements ItemPreparedStatementSetter<DBObject> {

	static final String QUERY_INSERT_USER = "INSERT INTO users (login, password, email, role, type, expired, full_name) VALUES (?, ?, ?, ?, ?, ?, ?);";

	@Override
	public void setValues(DBObject item, PreparedStatement ps) throws SQLException {
		ps.setString(1, (String) item.get("_id"));
		ps.setString(2, (String) item.get("password"));
		ps.setString(3, (String) item.get("email"));
		ps.setString(4, (String) item.get("role"));
		ps.setString(5, (String) item.get("type"));
		ps.setBoolean(6, (Boolean) item.get("isExpired"));
		ps.setString(7, Optional.ofNullable((String) item.get("fullName")).orElse(""));
	}
}
