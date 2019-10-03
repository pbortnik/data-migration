package com.epam.reportportal.migration.steps.utils;

import com.github.benmanes.caffeine.cache.Cache;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Collections;

import static com.epam.reportportal.migration.steps.items.TestProviderUtils.TICKETS_SOURCE_PROVIDER;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class CacheableDataService {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final String SELECT_PROJECT_ID = "SELECT id FROM project WHERE project.name = :name";

	private static final String SELECT_LAUNCH_ID = "SELECT id FROM launch WHERE launch.uuid = :uid";

	private static final String SELECT_ITEM_ID = "SELECT item_id FROM test_item WHERE test_item.uuid = :uid";

	private static final String SELECT_IDS = "SELECT item_id, launch_id FROM test_item WHERE test_item.uuid = :uid";

	private static final String INSERT_TICKET = "INSERT INTO ticket (ticket_id, submitter, submit_date, bts_url, bts_project, url) VALUES "
			+ "(:tid, :sub, :sd, :burl, :bpr, :url) RETURNING ticket.id";

	@Autowired
	private Cache<String, Object> idsCache;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	public Long retrieveProjectId(String projectName) {
		Long projectId = (Long) idsCache.getIfPresent(projectName);
		if (projectId == null) {
			try {
				projectId = jdbcTemplate.queryForObject(SELECT_PROJECT_ID, Collections.singletonMap("name", projectName), Long.class);
				idsCache.put(projectName, projectId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Project with name '%s' not found.", projectName));
				return null;
			}
		}
		return projectId;
	}

	public Long retrieveLaunchId(String launchRef) {
		if (launchRef == null) {
			return null;
		}
		Long launchId = (Long) idsCache.getIfPresent(launchRef);
		if (launchId == null) {
			try {
				launchId = jdbcTemplate.queryForObject(SELECT_LAUNCH_ID, Collections.singletonMap("uid", launchRef), Long.class);
				idsCache.put(launchRef, launchId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Launch with uuid '%s' not found. It is ignored.", launchRef));
				return null;
			}
		}
		return launchId;
	}

	public Long retrieveItemId(String itemRef) {
		if (itemRef == null) {
			return null;
		}
		Long itemId = (Long) idsCache.getIfPresent(itemRef);
		if (itemId == null) {
			try {
				itemId = jdbcTemplate.queryForObject(SELECT_ITEM_ID, Collections.singletonMap("uid", itemRef), Long.class);
				idsCache.put(itemRef, itemId);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Item with uuid '%s' not found. It is ignored.", itemRef));
				return null;
			}
		}
		return itemId;
	}

	public DBObject retrieveIds(String itemRef) {
		DBObject ids = null;
		Object object = idsCache.getIfPresent(itemRef);
		if (object instanceof DBObject) {
			ids = (DBObject) object;
		}
		if (ids == null) {
			try {
				ids = jdbcTemplate.query(SELECT_IDS, Collections.singletonMap("uid", itemRef), (ResultSetExtractor<DBObject>) rs -> {
					BasicDBObject dbObject = new BasicDBObject();
					if (rs.next()) {
						dbObject.put("itemId", rs.getLong("item_id"));
						dbObject.put("launchId", rs.getLong("launch_id"));
					} else {
						throw new EmptyResultDataAccessException(1);
					}
					return dbObject;
				});
				idsCache.put(itemRef, ids);
			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("TestItem with uuid '%s' not found. Log is ignored.", itemRef));
				return null;
			}
		}
		return ids;
	}

	public Long retrieveTicketId(DBObject ticket) {
		String btsTicketId = (String) ticket.get("ticketId");
		Long ticketId = (Long) idsCache.getIfPresent(btsTicketId);
		if (ticketId == null) {
			try {
				ticketId = jdbcTemplate.queryForObject("SELECT id FROM ticket WHERE ticket_id = :tid ",
						Collections.singletonMap("tid", btsTicketId),
						Long.class
				);
			} catch (Exception e) {
				ticketId = jdbcTemplate.queryForObject(INSERT_TICKET, TICKETS_SOURCE_PROVIDER.createSqlParameterSource(ticket), Long.class);
			}
			idsCache.put(btsTicketId, ticketId);
		}
		return ticketId;
	}
}
