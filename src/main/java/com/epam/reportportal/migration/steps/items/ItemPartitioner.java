package com.epam.reportportal.migration.steps.items;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Map;

import static com.epam.reportportal.migration.steps.utils.DatePartitioner.prepareExecutionContext;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public class ItemPartitioner implements Partitioner {

	private MongoOperations mongoOperations;

	private String keepFrom;

	private int pathLevel;

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Date fromDate = Date.from(LocalDate.parse(keepFrom).atStartOfDay(ZoneOffset.UTC).toInstant());
		Date minDate = new Date();
		Date maxDate = new Date();

		mongoOperations.executeQuery(
				Query.query(Criteria.where("start_time").gte(fromDate).and("pathLevel").is(pathLevel))
						.with(new Sort(Sort.Direction.ASC, "start_time"))
						.limit(1),
				ItemsStepConfig.OPTIMIZED_TEST_COLLECTION,
				dbObject -> minDate.setTime(((Date) dbObject.get("start_time")).getTime())
		);

		mongoOperations.executeQuery(
				Query.query(Criteria.where("start_time").gte(fromDate).and("pathLevel").is(pathLevel))
						.with(new Sort(Sort.Direction.DESC, "start_time"))
						.limit(1),
				ItemsStepConfig.OPTIMIZED_TEST_COLLECTION,
				dbObject -> maxDate.setTime(((Date) dbObject.get("start_time")).getTime())
		);

		Map<String, ExecutionContext> result = prepareExecutionContext(gridSize, minDate, maxDate);
		result.values().forEach(context -> context.putInt("pathLevel", pathLevel));
		return result;
	}

	public void setMongoOperations(MongoOperations mongoOperations) {
		this.mongoOperations = mongoOperations;
	}

	public void setKeepFrom(String keepFrom) {
		this.keepFrom = keepFrom;
	}

	public void setPathLevel(Integer pathLevel) {
		this.pathLevel = pathLevel;
	}
}
