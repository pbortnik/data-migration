package com.epam.reportportal.migration.steps.shareable.dashboard;

import com.epam.reportportal.migration.steps.shareable.ShareableUtilService;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
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
public class DashboardProcessor implements ItemProcessor<DBObject, DBObject> {

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
		retrieveWidgets((BasicDBList) item.get("widgets"));
		return item;
	}

	private void retrieveWidgets(BasicDBList widgets) {
		widgets.stream().map(DBObject.class::cast).forEach(widget -> {
			DBObject one = mongoTemplate.findOne(Query.query(Criteria.where("_id").is(widget.get("widgetId"))),
					DBObject.class,
					"widgetMapping"
			);
			if (one != null) {
				widget.putAll(one);
			}
		});
	}

}
