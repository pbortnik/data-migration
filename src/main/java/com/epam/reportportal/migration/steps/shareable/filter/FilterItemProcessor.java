package com.epam.reportportal.migration.steps.shareable.filter;

import com.epam.reportportal.migration.steps.shareable.ShareableUtilService;
import com.mongodb.DBObject;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class FilterItemProcessor implements ItemProcessor<DBObject, DBObject> {

	@Autowired
	private ShareableUtilService shareableUtilService;

	@Override
	public DBObject process(DBObject item) {
		boolean processed = shareableUtilService.processShareableEntity(item);
		if (!processed) {
			return null;
		}
		return item;
	}
}
