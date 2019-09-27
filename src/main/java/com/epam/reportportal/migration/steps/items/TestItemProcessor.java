package com.epam.reportportal.migration.steps.items;

import com.github.benmanes.caffeine.cache.Cache;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@StepScope
@Component("testItemProcessor")
public class TestItemProcessor implements ItemProcessor<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final String SELECT_LAUNCH_ID = "SELECT id FROM launch WHERE launch.uuid = :uid";

	public static final String SELECT_ITEM_ID = "SELECT item_id FROM test_item WHERE test_item.uuid = :uid";

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
		long start = System.currentTimeMillis();
		if (retrieveLaunch(item) == null) {
			return null;
		}
		retrieveParent(item);
		retrieveParentPath(item);
		retrieveIssue(item);
		System.out.println("Proccessing" + ((System.currentTimeMillis() - start)));
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
				System.out.println("id cache size " + idsCache.estimatedSize());
			} catch (EmptyResultDataAccessException e) {
				LOGGER.warn(String.format("Launch with uuid '%s' not found. It is ignored.", launchRef));
				return null;
			}
		}
		item.put("launchId", launchId);
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
				parentId = jdbcTemplate.queryForObject(SELECT_ITEM_ID, Collections.singletonMap("uid", parent), Long.class);
				idsCache.put(parent, parentId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.warn(String.format("Parent with uuid '%s' not found. It is ignored.", parent));
				return null;
			}
		}
		item.put("parentId", parentId);
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
				LOGGER.warn(String.format("Issue type with locator '%s' not found. It is ignored.", issue));
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
				LOGGER.warn(String.format("Bts with id '%s' not found. It is ignored.", systemId));
			}
		});
	}

	private void retrieveParentPath(DBObject item) {
		BasicDBList path = (BasicDBList) item.get("path");
		String pathStr;
		try {
			pathStr = path.stream().map(mongoId -> {
				Long id = idsCache.getIfPresent(mongoId);
				if (id == null) {
					id = jdbcTemplate.queryForObject(SELECT_ITEM_ID, Collections.singletonMap("uid", mongoId), Long.class);
				}
				return String.valueOf(id);
			}).collect(Collectors.joining("."));
			item.put("pathIds", pathStr);
		} catch (EmptyResultDataAccessException e) {
			LOGGER.warn(String.format("Item in path '%s' not found. Item is ignored.", path.toString()));
		}
	}
}
