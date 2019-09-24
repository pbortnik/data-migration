package com.epam.reportportal.migration.steps.items;

import com.github.benmanes.caffeine.cache.Cache;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("testItemProcessor")
public class TestItemProcessor implements ItemProcessor<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final String SELECT_LAUNCH_ID = "SELECT id FROM launch WHERE launch.uuid = :uid";

	private static final String SELECT_PARENT_ID = "SELECT item_id FROM test_item WHERE test_item.uuid = :uid";

	private static final String SELECT_ISSUE_TYPE_ID = "SELECT id FROM issue_type WHERE issue_type.locator = :loc";

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private Cache<String, Long> locatorsFieldsCache;

	@Autowired
	private Cache<String, Long> idsCache;

	@Override
	public DBObject process(DBObject item) {
		if (retrieveLaunch(item) == null) {
			return null;
		}
		if (retrieveParent(item) == null) {
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
				item.put("launchId", launchId);
				idsCache.put(launchRef, launchId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.warn(String.format("Launch with uuid '%s' not found. It is ignored.", launchRef));
				return null;
			}
		}
		return item;
	}

	private DBObject retrieveParent(DBObject item) {
		String parent = (String) item.get("parent");
		if (parent == null) {
			return item;
		}
		Long parentId = idsCache.getIfPresent(parent);
		if (parentId == null) {
			try {
				parentId = jdbcTemplate.queryForObject(SELECT_PARENT_ID, Collections.singletonMap("uid", parent), Long.class);
				item.put("parentId", parentId);
				idsCache.put(parent, parentId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.warn(String.format("Parent with uuid '%s' not found. It is ignored.", parent));
				return null;
			}
		}
		return item;
	}

	private void retrieveIssue(DBObject item) {
		if (item.get("issue") != null) {
			try {
				String locator = (String) ((DBObject) item.get("issue")).get("issueType");
				Long issueTypeId = locatorsFieldsCache.getIfPresent(locator);
				if (issueTypeId == null) {
					issueTypeId = jdbcTemplate.queryForObject(SELECT_ISSUE_TYPE_ID, Collections.singletonMap("loc", locator), Long.class);
				}
				item.put("issueTypeId", issueTypeId);
				locatorsFieldsCache.put(locator, issueTypeId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.warn(String.format("Issue type with locator '%s' not found. It is ignored.", item.get("issue")));
			}
		}
	}

}
