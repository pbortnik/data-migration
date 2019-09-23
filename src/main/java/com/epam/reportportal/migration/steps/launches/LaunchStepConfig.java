package com.epam.reportportal.migration.steps.launches;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.DBObject;
import org.springframework.batch.core.*;
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
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Date;
import java.time.Instant;
import java.util.Collections;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class LaunchStepConfig {

	@Value("${rp.launch.keepFor}")
	private Long keepFor;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private JdbcTemplate jdbcTemplate;

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
		if (keepFor != -1 && keepFor >= 0) {
			java.util.Date findFrom = Date.from(Instant.now().minusMillis(keepFor));
			itemReader.setQuery("{'last_modified': { $gte : ?0 }}");
			itemReader.setParameterValues(Collections.singletonList(findFrom));
		}
		return itemReader;
	}

	@Bean
	@StepScope
	public StepListener launchStepListener() {
		return new StepExecutionListener() {
			@Override
			public void beforeStep(StepExecution stepExecution) {
				jdbcTemplate.execute("ALTER TABLE launch DISABLE TRIGGER ALL;");
			}

			@Override
			public ExitStatus afterStep(StepExecution stepExecution) {
				jdbcTemplate.execute("ALTER TABLE launch ENABLE TRIGGER ALL;");
				return ExitStatus.COMPLETED;
			}
		};
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
