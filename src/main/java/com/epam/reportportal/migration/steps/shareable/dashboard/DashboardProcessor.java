package com.epam.reportportal.migration.steps.shareable.dashboard;

import com.epam.reportportal.migration.steps.shareable.ShareableUtilService;
import com.epam.reportportal.migration.steps.shareable.widget.WidgetProcessor;
import com.epam.reportportal.migration.steps.shareable.widget.WidgetWriter;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@SuppressWarnings("SuspiciousMethodCalls")
@Component
public class DashboardProcessor implements ItemProcessor<DBObject, DBObject> {

	@Autowired
	private WidgetProcessor widgetProcessor;

	@Autowired
	private WidgetWriter widgetWriter;

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
		List<Object> ids = widgets.stream().map(it -> ((DBObject) it).get("_id")).collect(Collectors.toList());
	}

}
