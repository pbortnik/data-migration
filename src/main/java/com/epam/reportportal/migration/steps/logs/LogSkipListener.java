package com.epam.reportportal.migration.steps.logs;

import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.SkipListener;
import org.springframework.stereotype.Component;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class LogSkipListener implements SkipListener<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@Override
	public void onSkipInRead(Throwable t) {

	}

	@Override
	public void onSkipInWrite(DBObject item, Throwable t) {
		LOGGER.warn("Skipping writing log with id " + item.get("_id").toString());
	}

	@Override
	public void onSkipInProcess(DBObject item, Throwable t) {

	}
}
