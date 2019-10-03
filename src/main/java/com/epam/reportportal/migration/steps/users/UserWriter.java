package com.epam.reportportal.migration.steps.users;

import com.epam.reportportal.migration.datastore.binary.DataStoreService;
import com.epam.reportportal.migration.datastore.binary.impl.DataStoreUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.gridfs.GridFsOperations;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.epam.reportportal.migration.datastore.binary.impl.DataStoreUtils.ATTACHMENT_CONTENT_TYPE;
import static com.epam.reportportal.migration.datastore.binary.impl.DataStoreUtils.ROOT_USER_PHOTO_DIR;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class UserWriter implements ItemWriter<DBObject> {

	static final String INSERT_USER = "INSERT INTO users (login, password, email, role, type, expired, full_name, metadata) VALUES "
			+ "(:lg, :pass, :em , :rl, :tp, :exp, :fn, :md::JSONB)";

	static final String INSERT_USER_ATTACH =
			"INSERT INTO users (login, attachment, attachment_thumbnail, password, email, role, type, expired, full_name, metadata) VALUES "
					+ "(:lg, :attach, :attach_thumb, :pass, :em , :rl, :tp, :exp, :fn, :md::JSONB)";

	@Autowired
	@Qualifier("userDataStoreService")
	private DataStoreService dataStoreService;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private GridFsOperations gridFsOperations;

	@Override
	public void write(List<? extends DBObject> items) {

		Map<Boolean, ? extends List<? extends DBObject>> splitted = items.stream()
				.collect(Collectors.partitioningBy(it -> it.get("photoId") != null));

		splitted.get(true).forEach(this::saveUserAndPhoto);

		SqlParameterSource[] parameterSources = splitted.get(false).stream().map(this::userParamSource).toArray(SqlParameterSource[]::new);
		jdbcTemplate.batchUpdate(INSERT_USER, parameterSources);

	}

	private SqlParameterSource userParamSource(DBObject user) {
		BasicDBObject metaInfo = (BasicDBObject) user.get("metaInfo");
		String metadata = "{\"metadata\": %s}";
		if (metaInfo != null) {
			metadata = String.format(metadata, metaInfo.toJson().replaceAll("lastLogin", "last_login"));
		} else {
			metadata = String.format(metadata, "{}");
		}

		MapSqlParameterSource ps = new MapSqlParameterSource();
		ps.addValue("lg", user.get("_id").toString());
		ps.addValue("pass", user.get("password"));
		ps.addValue("em", user.get("email"));
		ps.addValue("rl", user.get("role"));
		ps.addValue("tp", user.get("type"));
		ps.addValue("exp", user.get("isExpired"));
		ps.addValue("fn", Optional.ofNullable((String) user.get("fullName")).orElse(""));
		ps.addValue("md", metadata);
		return ps;
	}

	private void saveUserAndPhoto(DBObject user) {
		GridFSDBFile file = gridFsOperations.findOne(Query.query(Criteria.where("_id").is(user.get("photoId"))));
		String attach = dataStoreService.save(Paths.get(ROOT_USER_PHOTO_DIR, user.get("_id").toString()).toString(), file.getInputStream());
		String attachThumb = dataStoreService.saveThumbnail(DataStoreUtils.buildThumbnailFileName(ROOT_USER_PHOTO_DIR,
				user.get("_id").toString()
		), file.getInputStream());

		BasicDBObject metaInfo = (BasicDBObject) user.get("metaInfo");
		String metadata = "{\"metadata\": %s}";
		if (metaInfo != null) {
			metaInfo.put(ATTACHMENT_CONTENT_TYPE, file.getContentType());
			metadata = String.format(metadata, metaInfo.toJson().replaceAll("lastLogin", "last_login"));
		} else {
			metadata = String.format(metadata, "{}");
		}

		MapSqlParameterSource ps = new MapSqlParameterSource();
		ps.addValue("lg", user.get("_id").toString());
		ps.addValue("attach", attach);
		ps.addValue("attach_thumb", attachThumb);
		ps.addValue("pass", user.get("password"));
		ps.addValue("em", user.get("email"));
		ps.addValue("rl", user.get("role"));
		ps.addValue("tp", user.get("type"));
		ps.addValue("exp", user.get("isExpired"));
		ps.addValue("fn", Optional.ofNullable((String) user.get("fullName")).orElse(""));
		ps.addValue("md", metadata);

		jdbcTemplate.update(INSERT_USER_ATTACH, ps);
	}
}
