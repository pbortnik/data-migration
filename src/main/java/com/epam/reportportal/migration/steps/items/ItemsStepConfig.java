package com.epam.reportportal.migration.steps.items;

import com.epam.reportportal.migration.seek.MongoSeekItemReader;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
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
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class ItemsStepConfig {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	private static final int CHUNK_SIZE = 5_000;

	static String OPTIMIZED_TEST_COLLECTION = "optimizeTest";

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

	@Value("${rp.test.keepFrom}")
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
				OPTIMIZED_TEST_COLLECTION
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
	public ItemPartitioner partitioner(Integer i) {
		ItemPartitioner partitioner = new ItemPartitioner();
		partitioner.setKeepFrom(keepFrom);
		partitioner.setMongoOperations(mongoTemplate);
		partitioner.setPathLevel(i);
		return partitioner;
	}

	@Bean
	@Scope(BeanDefinition.SCOPE_PROTOTYPE)
	public Step slaveItemStep(int i) {
		return stepBuilderFactory.get("slaveItemStep." + i).<DBObject, DBObject>chunk(CHUNK_SIZE).reader(testItemReader(null,
				null,
				null,
				null,
				null
		)).processor(testItemProcessor).writer(testItemWriter).listener(chunkCountListener).build();
	}

	@Bean
	@StepScope
	public MongoSeekItemReader<DBObject> testItemReader(@Value("#{stepExecutionContext[minValue]}") Long minTime,
			@Value("#{stepExecutionContext[maxValue]}") Long maxTime, @Value("#{stepExecutionContext[pathLevel]}") Integer i,
			@Value("#{stepExecutionContext[firstId]}") ObjectId firstId, @Value("#{stepExecutionContext[latestId]}") ObjectId latestId) {
		MongoSeekItemReader<DBObject> itemReader = new MongoSeekItemReader<>();
		itemReader.setTemplate(mongoTemplate);
		itemReader.setTargetType(DBObject.class);
		itemReader.setCollection(ItemsStepConfig.OPTIMIZED_TEST_COLLECTION);
		itemReader.setCurrentObjectId(firstId);
		itemReader.setLatestObjectId(latestId);
		itemReader.setSort(new HashMap<String, Sort.Direction>() {{
			put("_id", Sort.Direction.ASC);
		}});
		itemReader.setQuery("{$and : [ { 'pathLevel' : ?0 }, { 'start_time': { $gte : ?1 }}, { 'start_time': { $lte : ?2 }}] }");
		itemReader.setLimit(CHUNK_SIZE);
		List<Object> paramValues = new LinkedList<>();
		paramValues.add(i);
		paramValues.add(new Date(minTime));
		paramValues.add(new Date(maxTime));
		itemReader.setParameterValues(paramValues);
		return itemReader;
	}

	private void prepareCollectionForMigration() {
		prepareIndexTestItemStartTime();
		prepareOptimizedTestItemCollection();
		prepareIndexOptimizedPath();
	}

	private void prepareIndexOptimizedPath() {
		List<DBObject> indexInfoOptimized = mongoTemplate.getCollection(OPTIMIZED_TEST_COLLECTION).getIndexInfo();
		if (indexInfoOptimized.stream().noneMatch(it -> ((String) it.get("name")).equalsIgnoreCase("start_time_path"))) {
			LOGGER.info("Adding 'start_time_path' index to optimizedTest collection");
			mongoTemplate.indexOps(OPTIMIZED_TEST_COLLECTION)
					.ensureIndex(new CompoundIndexDefinition(new BasicDBObject("start_time", 1).append("pathLevel", 1)).named(
							"start_time_path"));
			LOGGER.info("Adding 'start_time_path' index to optimizedTest collection successfully finished");
		}
		if (indexInfoOptimized.stream().noneMatch(it -> ((String) it.get("name")).equalsIgnoreCase("pathLevel_1"))) {
			LOGGER.info("Adding 'pathLevel_1' index to optimizedTest collection");
			mongoTemplate.indexOps(OPTIMIZED_TEST_COLLECTION).ensureIndex(new Index("pathLevel", Sort.Direction.ASC).named("pathLevel_1"));
			LOGGER.info("Adding 'pathLevel_1' index to optimizedTest collection successfully finished");
		}

	}

	private void prepareOptimizedTestItemCollection() {
		DBCollection optimizeTest = mongoTemplate.getCollection(OPTIMIZED_TEST_COLLECTION);
		if (optimizeTest == null) {
			mongoTemplate.createCollection(OPTIMIZED_TEST_COLLECTION);
		}

		Date fromDate = Date.from(LocalDate.parse(keepFrom).atStartOfDay(ZoneOffset.UTC).toInstant());

		LOGGER.info("Adding 'pathLevel' field to optimizeTest collection");
		mongoTemplate.aggregate(Aggregation.newAggregation(Aggregation.match(Criteria.where("start_time").gte(fromDate)),
				context -> new BasicDBObject("$addFields", new BasicDBObject("pathLevel", new BasicDBObject("$size", "$path"))),
				Aggregation.out(OPTIMIZED_TEST_COLLECTION)
		), "testItem", Object.class);

		LOGGER.info("Adding 'pathLevel' field to testItem collection successfully finished");
	}

	private void prepareIndexTestItemStartTime() {
		List<DBObject> indexInfo = mongoTemplate.getCollection("testItem").getIndexInfo();
		if (indexInfo.stream().noneMatch(it -> ((String) it.get("name")).equalsIgnoreCase("start_time"))) {
			LOGGER.info("Adding 'start_time' index to testItem collection");
			mongoTemplate.indexOps("testItem").ensureIndex(new Index("start_time", Sort.Direction.ASC).named("start_time"));
			LOGGER.info("Adding 'start_time' index to testItem collection successfully finished");
		}
	}

}
