package com.epam.reportportal.migration.steps.items;

import com.github.benmanes.caffeine.cache.Cache;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("testItemProcessor")
public class TestItemProcessor implements ItemProcessor<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final String SELECT_LAUNCH_ID = "SELECT id FROM launch WHERE launch.uuid = :uid";

	private static final String SELECT_ISSUE_TYPE_ID = "SELECT id FROM issue_type WHERE issue_type.locator = :loc";

	private static final String SELECT_BTS_ID = "SELECT id, params -> 'params' ->> 'project' AS project,  params -> 'params' ->> 'url' "
			+ "AS url FROM integration WHERE params -> 'params' ->> 'id' = :mid";

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private Cache<String, Long> locatorsFieldsCache;

	@Autowired
	private RowMapper<Map> btsRowMapper;

	@Autowired
	private Cache<String, Long> idsCache;

	@Override
	public DBObject process(DBObject item) {
		if (retrieveLaunch(item) == null) {
			return null;
		}
		retrieveIssue(item);
		return item;
	}

	private DBObject retrieveLaunch(DBObject item) {
		String launchRef = (String) item.get("launchRef");
		if (launchRef == null) {
			return null;
		}
		Long launchId = idsCache.getIfPresent(launchRef);
		if (launchId == null) {
			try {
				launchId = jdbcTemplate.queryForObject(SELECT_LAUNCH_ID, Collections.singletonMap("uid", launchRef), Long.class);
				idsCache.put(launchRef, launchId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Launch with uuid '%s' not found. It is ignored.", launchRef));
				return null;
			}
		}
		item.put("launchId", launchId);
		return item;
	}

	private void retrieveIssue(DBObject item) {
		DBObject issue = (DBObject) item.get("issue");
		if (issue != null) {
			try {
				String locator = ((String) issue.get("issueType")).toLowerCase();
				Long issueTypeId = locatorsFieldsCache.getIfPresent(locator);
				if (issueTypeId == null) {
					issueTypeId = jdbcTemplate.queryForObject(SELECT_ISSUE_TYPE_ID, Collections.singletonMap("loc", locator), Long.class);
					locatorsFieldsCache.put(locator, issueTypeId);
				}
				issue.put("issueTypeId", issueTypeId);
				BasicDBList tickets = (BasicDBList) issue.get("externalSystemIssues");
				if (!CollectionUtils.isEmpty(tickets)) {
					retrieveBts(tickets);
				}
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Issue type with locator '%s' not found. It is ignored.", issue));
			}
		}
	}

	private void retrieveBts(BasicDBList tickets) {
		tickets.forEach(item -> {
			String systemId = (String) ((DBObject) item).get("externalSystemId");
			try {
				Map bts = jdbcTemplate.queryForObject(SELECT_BTS_ID, Collections.singletonMap("mid", systemId), btsRowMapper);
				((DBObject) item).putAll(bts);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Bts with id '%s' not found. It is ignored.", systemId));
			}
		});
	}
}
