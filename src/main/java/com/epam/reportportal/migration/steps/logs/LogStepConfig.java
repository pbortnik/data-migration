package com.epam.reportportal.migration.steps.logs;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class LogStepConfig {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final int CHUNK_SIZE = 5_000;

	@Value("${rp.log.keepFrom}")
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
	public Step migrateLogStep() {
		prepareCollectionForReading();
		return stepBuilderFactory.get("log")
				.partitioner("slaveLogStep", logPartitioner)
				.gridSize(5)
				.step(slaveLogStep())
				.taskExecutor(threadPoolTaskExecutor)
				.listener(chunkCountListener)
				.build();
	}

	@Bean
	public Step slaveLogStep() {
		return stepBuilderFactory.get("slaveLogStep").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(logItemReader(null, null))
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

	private void prepareCollectionForReading() {
		if (mongoTemplate.getCollection("log")
				.getIndexInfo()
				.stream()
				.noneMatch(it -> ((String) it.get("name")).equalsIgnoreCase("log_time"))) {
			LOGGER.info("Adding 'log_time' index to log collection");
			mongoTemplate.indexOps("log").ensureIndex(new Index("log_time", Sort.Direction.ASC).named("log_time"));
			LOGGER.info("Adding 'log_time' index to log collection successfully finished");
		}
	}
}
