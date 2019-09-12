package com.epam.reportportal.migration.steps.users;

import com.mongodb.DBObject;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.util.HashMap;

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

	@Bean
	public MongoItemReader<DBObject> userMongoItemReader() {
		MongoItemReader<DBObject> mongoItemReader = new MongoItemReader<>();
		mongoItemReader.setTemplate(mongoTemplate);
		mongoItemReader.setTargetType(DBObject.class);
		mongoItemReader.setCollection("user");
		mongoItemReader.setSort(new HashMap<String, Sort.Direction>() {{
			put("_id", Sort.Direction.ASC);
		}});
		mongoItemReader.setQuery("{}");
		return mongoItemReader;
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
		return stepBuilderFactory.get("user").<DBObject, DBObject>chunk(10).reader(userMongoItemReader())
				.processor(userItemProcessor())
				.writer(userItemWriter())
				.build();
	}


}
