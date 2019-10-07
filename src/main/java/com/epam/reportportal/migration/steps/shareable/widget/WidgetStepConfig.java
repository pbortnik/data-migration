package com.epam.reportportal.migration.steps.shareable.widget;

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
public class WidgetStepConfig {

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
	private ItemProcessor widgetItemProcessor;

	@Autowired
	private ItemWriter widgetItemWriter;

	@Bean
	public MongoItemReader<DBObject> widgetItemReader() {
		MongoItemReader<DBObject> reader = MigrationUtils.getMongoItemReader(mongoTemplate, "widget");
		reader.setPageSize(CHUNK_SIZE);
		return reader;
	}

	@Bean
	public Step migrateDashboardStep() {
		return stepBuilderFactory.get("widget").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(widgetItemReader())
				.processor(widgetItemProcessor)
				.writer(widgetItemWriter)
				.listener(chunkCountListener)
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}

}
