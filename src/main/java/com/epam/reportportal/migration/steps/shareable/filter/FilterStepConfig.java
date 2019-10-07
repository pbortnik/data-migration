package com.epam.reportportal.migration.steps.shareable.filter;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.DBObject;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class FilterStepConfig {

	private static final int CHUNK_SIZE = 1000;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private ChunkListener chunkCountListener;

	@Autowired
	private ThreadPoolTaskExecutor threadPoolTaskExecutor;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private ItemProcessor filterItemProcessor;

	@Autowired
	private ItemWriter filterItemWriter;

	@Bean
	public MongoItemReader<DBObject> filterItemReader() {
		MongoItemReader<DBObject> project = MigrationUtils.getMongoItemReader(mongoTemplate, "userFilter");
		project.setPageSize(CHUNK_SIZE);
		return project;
	}

	@Bean
	public Step migrateProjectsStep() {
		return stepBuilderFactory.get("project").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(filterItemReader())
				.processor(filterItemProcessor)
				.writer(filterItemWriter)
				.listener(chunkCountListener)
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}

}
