package com.epam.reportportal.migration.steps.logs;

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
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public class LogStepConfig {

	private static final int CHUNK_SIZE = 5_000;


	@Value("${rp.launch.keepFrom}")
	private String keepFrom;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	@Qualifier("logProcessor")
	private ItemProcessor<DBObject, DBObject> logProcessor;

	@Autowired
	@Qualifier("logWriter")
	private ItemWriter<DBObject> logWriter;

	@Autowired
	@Qualifier("chunkCountListener")
	private ChunkListener chunkCountListener;

	@Autowired
	private Partitioner logPartitioner;

	@Autowired
	private TaskExecutor threadPoolTaskExecutor;

	@Bean(name = "migrateLogStep")
	public Step migrateLaunchStep() {
		return stepBuilderFactory.get("log")
				.partitioner("slaveLogStep", logPartitioner)
				.gridSize(12)
				.step(slaveLogStep())
				.taskExecutor(threadPoolTaskExecutor)
				.listener(chunkCountListener)
				.build();
	}

	@Bean
	public Step slaveLogStep() {
		return stepBuilderFactory.get("slaveLaunchStep").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(logItemReader(null, null))
				.processor(logProcessor)
				.writer(logWriter)
				.build();
	}

	@Bean
	@StepScope
	public MongoItemReader<DBObject> logItemReader(@Value("#{stepExecutionContext[minValue]}") Long minTime,
			@Value("#{stepExecutionContext[maxValue]}") Long maxTime) {
		MongoItemReader<DBObject> itemReader = MigrationUtils.getMongoItemReader(mongoTemplate, "log");
		itemReader.setQuery("{ $and : [ { 'logTime': { $gte : ?0 }}, { 'logTime': { $lte :  ?1 }} ]}");
		List<Object> list = new LinkedList<>();
		list.add(new Date(minTime));
		list.add(new Date(maxTime));
		itemReader.setParameterValues(list);
		itemReader.setPageSize(CHUNK_SIZE);
		return itemReader;
	}
}
