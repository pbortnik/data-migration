package com.epam.reportportal.migration.steps.shareable.widget;

import com.epam.reportportal.migration.steps.shareable.ShareableUtilService;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class WidgetProcessor implements ItemProcessor<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private ShareableUtilService shareableUtilService;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Override
	public DBObject process(DBObject item) {
		boolean processed = shareableUtilService.processShareableEntity(item);
		if (!processed) {
			return null;
		}
		if (!StringUtils.isEmpty(item.get("applyingFilterId"))) {
			Long filterId = processFilter(item);
			if (filterId == null) {
				LOGGER.debug(String.format("Filter with id %s not found. Skipping widget.", item.get("applyingFilterId")));
				return null;
			}
			item.put("filterId", filterId);
		}
		return item;
	}

	private Long processFilter(DBObject item) {
		DBObject one = mongoTemplate.findOne(
				Query.query(Criteria.where("_id").is(new ObjectId(item.get("applyingFilterId").toString()))),
				DBObject.class,
				"filterMapping"
		);
		if (one == null) {
			return null;
		}
		return (Long) one.get("postgresId");
	}
}
