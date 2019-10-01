package com.epam.reportportal.migration.steps.logs;

import com.epam.reportportal.migration.steps.utils.CacheableDataService;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFSDBFile;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.gridfs.GridFsOperations;
import org.springframework.stereotype.Component;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class LogProcessor implements ItemProcessor<DBObject, DBObject> {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private GridFsOperations gridFs;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Override
	public DBObject process(DBObject log) {
		if (retrieveIds(log) == null) {
			return null;
		}
		retrieveBinaryContent(log);
		return log;
	}

	private void retrieveBinaryContent(DBObject log) {
		BasicDBObject binaryContent = (BasicDBObject) log.get("binary_content");
		if (binaryContent != null) {
			GridFSDBFile file = gridFs.findOne(Query.query(Criteria.where("_id").is(new ObjectId((String) binaryContent.get("id")))));
			log.put("file", file);
		}
	}

	private DBObject retrieveIds(DBObject log) {
		DBObject ids = cacheableDataService.retrieveIds((String) log.get("testItemRef"));
		if (ids == null) {
			return null;
		}
		log.putAll(ids);
		return log;
	}
}
