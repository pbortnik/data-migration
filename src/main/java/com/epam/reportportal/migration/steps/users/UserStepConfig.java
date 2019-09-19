package com.epam.reportportal.migration.steps.users;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.DBObject;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class UserStepConfig {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private DataSource dataSource;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	@Qualifier("chunkCountListener")
	private ChunkListener chunkCountListener;

	@Bean
	public MongoItemReader<DBObject> userMongoItemReader() {
		return MigrationUtils.getMongoItemReader(mongoTemplate, "user");
	}

	@Bean
	public ItemProcessor<DBObject, DBObject> userItemProcessor() {
		return item -> item;
	}

	@Bean
	public ItemWriter<DBObject> userItemWriter() {
		JdbcBatchItemWriter<DBObject> writer = new JdbcBatchItemWriter<>();
		writer.setDataSource(dataSource);
		writer.setJdbcTemplate(jdbcTemplate);
		writer.setSql(UserPreparedStatementSetter.QUERY_INSERT_USER);
		writer.setItemPreparedStatementSetter(new UserPreparedStatementSetter());
		return writer;
	}

	@Bean
	public Step migrateUsersStep() {
		return stepBuilderFactory.get("user").<DBObject, DBObject>chunk(200).reader(userMongoItemReader())
				.processor(userItemProcessor())
				.writer(userItemWriter())
				.listener(chunkCountListener)
				.build();
	}

}
