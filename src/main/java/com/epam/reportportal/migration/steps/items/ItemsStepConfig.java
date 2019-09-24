package com.epam.reportportal.migration.steps.items;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.DBObject;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
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

	@Value("${rp.launch.keepFor}")
	private Long keepFor;

	@Bean
	@StepScope
	public MongoItemReader<DBObject> testItemReader() {
		MongoItemReader<DBObject> itemReader = MigrationUtils.getMongoItemReader(mongoTemplate, "testItem");
		if (keepFor != -1 && keepFor >= 0) {
			java.util.Date findFrom = Date.from(Instant.now().minusMillis(keepFor));
			itemReader.setQuery("{'start_time': { $gte : ?0 }}");
			itemReader.setParameterValues(Collections.singletonList(findFrom));
		}
		return itemReader;
	}

	@Bean(name = "migrateTestItemStep")
	public Step migrateTestItemStep() {
		return stepBuilderFactory.get("testItem").<DBObject, DBObject>chunk(50).reader(testItemReader())
				.processor(testItemProcessor)
				.writer(testItemWriter)
				.listener(chunkCountListener)
				.build();
	}

}
