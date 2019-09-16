package com.epam.reportportal.migration.steps.bts;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.DBObject;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class BtsStepConfig {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Bean
	public MongoItemReader<DBObject> btsMongoReader() {
		return MigrationUtils.getMongoItemReader(mongoTemplate, "bts");
	}

	@Bean
	public ItemProcessor<DBObject, DBObject> btsItemProcessor() {
		return item -> item;
	}

	@Bean
	public ItemWriter<DBObject> btsItemWriter() {
		return null;
	}

	@Bean
	public Step migrateBtsStep() {
		return stepBuilderFactory.get("bts").<DBObject, DBObject>chunk(10).reader(btsMongoReader())
				.processor(btsItemProcessor())
				.writer(btsItemWriter())
				.build();
	}

}
