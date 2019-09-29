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
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
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
		int pathSize = 0;
		while (true) {
			boolean exists = mongoTemplate.exists(Query.query(Criteria.where("path").size(pathSize)), "testItem");
			if (!exists) {
				pathSize--;
				break;
			}
			pathSize++;
		}

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
				.gridSize(5)
				.step(slaveItemStep(i))
				.taskExecutor(threadPoolTaskExecutor)
				.listener(chunkCountListener)
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
		return stepBuilderFactory.get("slaveItemStep." + i).<DBObject, DBObject>chunk(1000)
				.reader(testItemReader(null, null, null))
				.processor(testItemProcessor)
				.writer(testItemWriter)
				.build();
	}

	@Bean
	@StepScope
	public MongoItemReader<DBObject> testItemReader(@Value("#{stepExecutionContext[minValue]}") Long minTime,
			@Value("#{stepExecutionContext[maxValue]}") Long maxTime, @Value("#{stepExecutionContext[pathLevel]}") Integer i) {
		MongoItemReader<DBObject> itemReader = MigrationUtils.getMongoItemReader(mongoTemplate, "testItem");
		itemReader.setQuery("{$and : [ { 'path' : {$size : ?0 }}, { 'start_time': { $gte : ?1 }}, { 'start_time': { $lte : ?2 }}] }");
		List<Object> paramValues = new LinkedList<>();
		paramValues.add(i);
		paramValues.add(new Date(minTime));
		paramValues.add(new Date(maxTime));
		itemReader.setParameterValues(paramValues);
		return itemReader;
	}

}
