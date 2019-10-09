package com.epam.reportportal.migration.steps.shareable.widget;

import com.epam.reportportal.migration.steps.shareable.ShareableUtilService;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class WidgetProcessor implements ItemProcessor<DBObject, DBObject> {

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
		Long filterId = processFilter(item);
		if (filterId == null) {
			return null;
		}
		item.put("filterId", filterId);
		return item;
	}

	private Long processFilter(DBObject item) {
		return (Long) mongoTemplate.findOne(
				Query.query(Criteria.where("_id").is(new ObjectId(item.get("applyingFilterId").toString()))),
				DBObject.class,
				"filterMapping"
		).get("postgresId");
	}
}
