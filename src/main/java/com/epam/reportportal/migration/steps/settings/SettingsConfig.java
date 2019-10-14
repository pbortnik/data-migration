package com.epam.reportportal.migration.steps.settings;

import com.epam.reportportal.migration.steps.utils.CacheableDataService;
import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.DBObject;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public class SettingsConfig {

	private static final String INSERT_USER_PREFERENCES = "INSERT INTO user_preference (project_id, user_id, filter_id) VALUES (?,?,?)";

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Bean
	public MongoItemReader<DBObject> settingsReader() {
		return MigrationUtils.getMongoItemReader(mongoTemplate, "serverSettings");
	}

	@Bean
	public ItemProcessor<DBObject, DBObject> settingsProcessor() {
		return item -> item;
	}

	@Bean
	public ItemWriter<? super DBObject> settingsWriter() {
		return items -> {
		};
	}

	@Bean
	public Step migratePreferencesStep() {
		return stepBuilderFactory.get("settings").<DBObject, DBObject>chunk(1).reader(settingsReader())
				.processor(settingsProcessor())
				.writer(settingsWriter())
				.build();
	}

}
