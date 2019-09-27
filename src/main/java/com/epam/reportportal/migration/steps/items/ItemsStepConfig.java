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
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;

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

	@Value("${rp.launch.keepFrom}")
	private String keepFrom;

	@Bean
	@StepScope
	public MongoItemReader<DBObject> testItemReader() {
		MongoItemReader<DBObject> itemReader = MigrationUtils.getMongoItemReader(mongoTemplate, "testItem");
		java.util.Date dateFrom = Date.from(LocalDate.parse(keepFrom).atStartOfDay(ZoneOffset.UTC).toInstant().minus(5, ChronoUnit.DAYS));
		itemReader.setQuery("{'start_time': { $gte : ?0 }}");
		itemReader.setParameterValues(Collections.singletonList(dateFrom));
		itemReader.setSort(new HashMap<String, Sort.Direction>() {{
			put("start_time", Sort.Direction.ASC);
		}});
		return itemReader;
	}

	@Bean(name = "migrateTestItemStep")
	public Step migrateTestItemStep() {
		return stepBuilderFactory.get("testItem").<DBObject, DBObject>chunk(1000).reader(testItemReader())
				.processor(testItemProcessor)
				.writer(testItemWriter)
				.listener(chunkCountListener)
				.build();
	}

}
