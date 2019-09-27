package com.epam.reportportal.migration.steps.items;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.DBObject;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class ItemsStepConfig {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	@Qualifier("chunkCountListener")
	private ChunkListener chunkCountListener;

	@Autowired
	@Qualifier("testItemProcessor")
	private ItemProcessor testItemProcessor;

	@Autowired
	@Qualifier("testItemWriter")
	private ItemWriter testItemWriter;

	@Value("${rp.launch.keepFrom}")
	private String keepFrom;

	@Autowired
	@Qualifier("threadPoolTaskExecutor")
	private ThreadPoolTaskExecutor threadPoolTaskExecutor;

	@Bean
	public List<Step> levelItemsFlow() {
		int pathSize = 0;
		while (true) {
			boolean exists = mongoTemplate.exists(Query.query(Criteria.where("path").size(pathSize)), "testItem");
			if (!exists) {
				pathSize--;
				break;
			}
			pathSize++;
		}

		List<Step> steps = new LinkedList<>();
		for (int i = 0; i <= pathSize; i++) {
			Step step = migrateTestItemStep(i);
			steps.add(step);
		}
		return steps;
	}

	public Step migrateTestItemStep(int pathLevel) {
		return stepBuilderFactory.get("testStep." + pathLevel).<DBObject, DBObject>chunk(100).reader(testItemReader(pathLevel))
				.processor(testItemProcessor)
				.writer(testItemWriter)
				.taskExecutor(threadPoolTaskExecutor)
				.listener(chunkCountListener)
				.build();
	}

	public MongoItemReader<DBObject> testItemReader(int pathLevel) {
		MongoItemReader<DBObject> itemReader = MigrationUtils.getMongoItemReader(mongoTemplate, "testItem");
		Date fromDate = Date.from(LocalDate.parse(keepFrom).atStartOfDay(ZoneOffset.UTC).toInstant());
		itemReader.setQuery("{$and : [ { 'path' : {$size : ?0 }}, { 'start_time': { $gte : ?1 }}] }");
		List<Object> paramValues = new LinkedList<>();
		paramValues.add(pathLevel);
		paramValues.add(fromDate);
		itemReader.setParameterValues(paramValues);
		return itemReader;
	}

}
