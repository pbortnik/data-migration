package com.epam.reportportal.migration.steps.launches;

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
import org.springframework.data.mongodb.core.MongoTemplate;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Date;

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


	@Bean
	@StepScope
	public MongoItemReader<DBObject> launchItemReader() {
		MongoItemReader<DBObject> itemReader = MigrationUtils.getMongoItemReader(mongoTemplate, "launch");
		java.util.Date dateFrom = Date.from(LocalDate.parse(keepFrom).atStartOfDay(ZoneOffset.UTC).toInstant());
		itemReader.setQuery("{'last_modified': { $gte : ?0 }}");
		itemReader.setParameterValues(Collections.singletonList(dateFrom));
		return itemReader;
	}

	@Bean(name = "migrateLaunchStep")
	public Step migrateLaunchStep() {
		return stepBuilderFactory.get("launch").<DBObject, DBObject>chunk(50).reader(launchItemReader())
				.processor(launchItemProcessor)
				.writer(launchItemWriter)
				.listener(chunkCountListener)
				.build();
	}

}
