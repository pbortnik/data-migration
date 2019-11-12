package com.epam.reportportal.migration.steps.users;

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
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class AccessTokenConfig {

	private static final String INSERT_USER_TOKENS = "INSERT INTO oauth_access_token (token_id, token, authentication_id, username, user_id, client_id, authentication, refresh_token) VALUES(?,?,?,?,?,?,?,?)";

	public static final int CHUNK_SIZE = 100;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private TaskExecutor threadPoolTaskExecutor;

	@Bean
	public MongoItemReader<DBObject> tokensReader() {
		MongoItemReader<DBObject> reader = MigrationUtils.getMongoItemReader(mongoTemplate, "oauth_access_token");
		reader.setPageSize(CHUNK_SIZE);
		return reader;
	}

	@Bean
	public ItemProcessor<DBObject, DBObject> tokensProcessor() {
		return item -> item;
	}

	@Bean
	public ItemWriter<? super DBObject> tokensWriter() {
		return items -> {
			List<Object[]> params = items.stream()
					.map(it -> new Object[] { it.get("_id"), it.get("token"), it.get("authenticationId"), it.get("userName"),
							cacheableDataService.retrieveUser((String) it.get("userName")), it.get("clientId"), it.get("authentication"),
							it.get("refreshToken") })
					.collect(Collectors.toList());
			jdbcTemplate.batchUpdate(INSERT_USER_TOKENS, params);
		};
	}

	@Bean
	public Step migrateTokensStep() {
		return stepBuilderFactory.get("tokens").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(tokensReader())
				.processor(tokensProcessor())
				.writer(tokensWriter())
				.taskExecutor(threadPoolTaskExecutor)
				.build();
	}

}
