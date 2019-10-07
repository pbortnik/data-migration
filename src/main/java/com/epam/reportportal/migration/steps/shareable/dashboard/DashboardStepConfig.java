package com.epam.reportportal.migration.steps.shareable.dashboard;

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

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
//@Component
public class DashboardStepConfig {

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
	private ItemProcessor dashboardItemProcessor;

	@Autowired
	private ItemWriter dashboardItemWriter;

	@Bean
	public MongoItemReader<DBObject> dashboardItemReader() {
		MongoItemReader<DBObject> reader = MigrationUtils.getMongoItemReader(mongoTemplate, "dashboard");
		reader.setPageSize(CHUNK_SIZE);
		return reader;
	}

	@Bean
	public Step migrateDashboardStep() {
		return stepBuilderFactory.get("dashboard").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(dashboardItemReader())
				.processor(dashboardItemProcessor)
				.writer(dashboardItemWriter)
				.listener(chunkCountListener)
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}
}
