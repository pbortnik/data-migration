package com.epam.reportportal.migration.steps.bts;

import com.mongodb.DBObject;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("btsItemWriter")
public class BtsItemWriter implements ItemWriter<DBObject> {

	@Override
	public void write(List<? extends DBObject> items) throws Exception {

	}
}
