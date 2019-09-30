package com.epam.reportportal.migration.steps.items;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.BasicDBObject;
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
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.index.CompoundIndexDefinition;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class ItemsStepConfig {

	private static final int CHUNK_SIZE = 5_000;

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

	@Autowired
	@Qualifier("threadPoolTaskExecutor")
	private ThreadPoolTaskExecutor threadPoolTaskExecutor;

	@Value("${rp.launch.keepFrom}")
	private String keepFrom;

	@Bean
	public Function<Integer, Step> itemStepFactory() {
		return this::migrateItemStep;
	}

	@Bean
	public List<Step> levelItemsFlow() {

		prepareCollectionForMigration();

		DBObject testItem = mongoTemplate.findOne(new Query().with(new Sort(Sort.Direction.DESC, "pathLevel")).limit(1),
				DBObject.class,
				"testItem"
		);
		int pathSize = (int) testItem.get("pathLevel");
		List<Step> steps = new LinkedList<>();
		for (Integer i = 0; i <= pathSize; i++) {
			Step step = itemStepFactory().apply(i);
			steps.add(step);
		}
		return steps;
	}

	@Bean
	@Scope(BeanDefinition.SCOPE_PROTOTYPE)
	public Step migrateItemStep(Integer i) {
		return stepBuilderFactory.get("item." + i)
				.partitioner("slaveItemStep." + i, partitioner(i))
				.gridSize(12)
				.step(slaveItemStep(i))
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}

	@Bean
	@Scope(BeanDefinition.SCOPE_PROTOTYPE)
	public ItemDatePartitioner partitioner(Integer i) {
		ItemDatePartitioner partitioner = new ItemDatePartitioner();
		partitioner.setKeepFrom(keepFrom);
		partitioner.setMongoOperations(mongoTemplate);
		partitioner.setPathLevel(i);
		return partitioner;
	}

	@Bean
	@Scope(BeanDefinition.SCOPE_PROTOTYPE)
	public Step slaveItemStep(int i) {
		return stepBuilderFactory.get("slaveItemStep." + i).<DBObject, DBObject>chunk(CHUNK_SIZE).reader(testItemReader(null, null, null))
				.processor(testItemProcessor)
				.writer(testItemWriter)
				.listener(chunkCountListener)
				.build();
	}

	@Bean
	@StepScope
	public MongoItemReader<DBObject> testItemReader(@Value("#{stepExecutionContext[minValue]}") Long minTime,
			@Value("#{stepExecutionContext[maxValue]}") Long maxTime, @Value("#{stepExecutionContext[pathLevel]}") Integer i) {
		MongoItemReader<DBObject> itemReader = MigrationUtils.getMongoItemReader(mongoTemplate, "testItem");
		itemReader.setQuery("{$and : [ { 'path' : {$size : ?0 }}, { 'start_time': { $gte : ?1 }}, { 'start_time': { $lte : ?2 }}] }");
		itemReader.setPageSize(CHUNK_SIZE);
		List<Object> paramValues = new LinkedList<>();
		paramValues.add(i);
		paramValues.add(new Date(minTime));
		paramValues.add(new Date(maxTime));
		itemReader.setParameterValues(paramValues);
		return itemReader;
	}

	private void prepareCollectionForMigration() {
		long start = System.currentTimeMillis();

		DBObject testItem = mongoTemplate.findOne(new Query().limit(1), DBObject.class, "testItem");
		if (null == testItem.get("pathLevel")) {
			mongoTemplate.aggregate(Aggregation.newAggregation(
					context -> new BasicDBObject("$addFields", new BasicDBObject("pathLevel", new BasicDBObject("$size", "$path"))),
					Aggregation.out("testItem")
			), "testItem", Object.class);
			mongoTemplate.indexOps("testItem").ensureIndex(new Index("start_time", Sort.Direction.ASC));
			mongoTemplate.indexOps("testItem")
					.ensureIndex(new CompoundIndexDefinition(new BasicDBObject("start_time", 1).append("pathLevel", 1)));
		}

		System.err.println(System.currentTimeMillis() - start);

	}

}
