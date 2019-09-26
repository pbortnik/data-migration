package com.epam.reportportal.migration.steps.launches;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.DBObject;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class LaunchStepConfig {

	@Value("${rp.launch.keepFrom}")
	private String keepFrom;

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

	@Autowired
	private Partitioner datePartitioning;

	@Bean
	@StepScope
	public MongoItemReader<DBObject> launchItemReader(@Value("#{stepExecutionContext[minValue]}") Long minTime,
			@Value("#{stepExecutionContext[maxValue]}") Long maxTime) {
		MongoItemReader<DBObject> itemReader = MigrationUtils.getMongoItemReader(mongoTemplate, "launch");
		itemReader.setQuery("{ $and : [ { 'start_time': { $gte : ?0 }}, { 'start_time': { $lte :  ?1 }} ]}");
		List<Object> list = new LinkedList<>();
		list.add(new Date(minTime));
		list.add(new Date(maxTime));
		itemReader.setParameterValues(list);
		return itemReader;
	}

	@Bean(name = "migrateLaunchStep")
	public Step migrateLaunchStep() {
		return stepBuilderFactory.get("launch")
				.partitioner("slaveLaunchStep", datePartitioning)
				.gridSize(12)
				.step(slaveStep())
				.taskExecutor(new SimpleAsyncTaskExecutor())
				.listener(chunkCountListener)
				.build();
	}

	@Bean
	public Step slaveStep() {
		return stepBuilderFactory.get("slaveLaunchStep").<DBObject, DBObject>chunk(50).reader(launchItemReader(null, null))
				.processor(launchItemProcessor)
				.writer(launchItemWriter)
				.build();
	}

	//	@Bean(name = "migrateLaunchStep")
	//	public Step migrateLaunchStep() {
	//		return stepBuilderFactory.get("launch").<DBObject, DBObject>chunk(50).reader(launchItemReader())
	//				.processor(launchItemProcessor)
	//				.writer(launchItemWriter)
	//				.listener(chunkCountListener)
	//				.build();
	//	}

}
