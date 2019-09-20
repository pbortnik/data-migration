package com.epam.reportportal.migration.steps.launches;

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

import java.sql.Date;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class LaunchStepConfig {

	@Value("${rp.launch.keepFor}")
	private Long keepFor;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("launchItemProcessor")
	private ItemProcessor<DBObject, DBObject> launchItemProcessor;

	@Autowired
	@Qualifier("launchItemWriter")
	private ItemWriter<DBObject> launchItemWriter;

	@Autowired
	@Qualifier("chunkCountListener")
	private ChunkListener chunkCountListener;

	@Bean(name = "statisticsFields")
	// Default statistics fields have fixed ids in PostgreSQL
	public Map<String, Long> statisticsFields() {
		Map<String, Long> statisticsFields = new HashMap<>(14);
		statisticsFields.put("statistics$executions$total", 1L);
		statisticsFields.put("statistics$executions$passed", 2L);
		statisticsFields.put("statistics$executions$skipped", 3L);
		statisticsFields.put("statistics$executions$failed", 4L);
		statisticsFields.put("statistics$defects$automation_bug$total", 5L);
		statisticsFields.put("statistics$defects$automation_bug$ab001", 6L);
		statisticsFields.put("statistics$defects$product_bug$total", 7L);
		statisticsFields.put("statistics$defects$product_bug$pb001", 8L);
		statisticsFields.put("statistics$defects$system_issue$total", 9L);
		statisticsFields.put("statistics$defects$system_issue$si001", 10L);
		statisticsFields.put("statistics$defects$to_investigate$total", 11L);
		statisticsFields.put("statistics$defects$to_investigate$ti001", 12L);
		statisticsFields.put("statistics$defects$no_defect$total", 13L);
		statisticsFields.put("statistics$defects$no_defect$nd001", 14L);
		return statisticsFields;
	}

	@Bean
	public MongoItemReader<DBObject> launchItemReader() {
		MongoItemReader<DBObject> itemReader = MigrationUtils.getMongoItemReader(mongoTemplate, "launch");
		java.util.Date findFrom = Date.from(Instant.now().minusMillis(keepFor));
		itemReader.setQuery("{'last_modified': { $gte : ?0 }}");
		itemReader.setParameterValues(Collections.singletonList(findFrom));
		return itemReader;
	}

	@Bean(name = "migrateLaunchStep")
	public Step migrateLaunchStep() {
		return stepBuilderFactory.get("launch").<DBObject, DBObject>chunk(50).reader(launchItemReader())
				.processor(launchItemProcessor)
				.writer(launchItemWriter)
				.listener(chunkCountListener)
				.build();
	}

}
