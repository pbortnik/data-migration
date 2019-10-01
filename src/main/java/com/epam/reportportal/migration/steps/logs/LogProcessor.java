package com.epam.reportportal.migration.steps.logs;

import com.github.benmanes.caffeine.cache.Cache;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class LogProcessor implements ItemProcessor<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	public static final String SELECT_ITEM = "SELECT item_id, launch_id FROM test_item WHERE test_item.uuid = :uid";

	@Autowired
	private Cache<String, Long> idsCache;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Override
	public DBObject process(DBObject log) {
		String testUuid = (String) log.get("testItemRef");
		Long testId = idsCache.getIfPresent(testUuid);
		if (testId == null) {
			try {
				jdbcTemplate.query(SELECT_ITEM, Collections.singletonMap("uuid", testUuid), new RowCallbackHandler() {
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						log.put("itemId", rs.getLong("item_id"));
						log.put("launchId", rs.getLong("launch_id"));
					}
				});
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("TestItem with uuid '%s' not found. Log is ignored.", testUuid));
				return null;
			}
		}
		log.put("itemId", testId);
		return log;
	}
}
