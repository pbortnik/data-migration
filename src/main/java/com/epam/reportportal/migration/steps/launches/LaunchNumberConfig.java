package com.epam.reportportal.migration.steps.launches;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;

import java.util.Collections;
import java.util.Map;

import static com.epam.reportportal.migration.steps.utils.MigrationUtils.SELECT_PROJECT_ID;
import static java.util.stream.Collectors.toList;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class LaunchNumberConfig {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private ChunkListener chunkListener;

	@Bean
	public ItemReader<DBObject> launchNumberReader() {
		return MigrationUtils.getMongoItemReader(mongoTemplate, "launchMetaInfo");
	}

	private static final String SELECT_LAUNCH_EXISTS = "SELECT exists(SELECT 1 FROM launch WHERE name=:nm)";

	@Bean
	public ItemProcessor<DBObject, DBObject> launchNumberProcessor() {
		return it -> {
			DBObject projects = (DBObject) it.get("projects");
			if (null == projects) {
				return null;
			}
			Boolean launchExists = jdbcTemplate.queryForObject(SELECT_LAUNCH_EXISTS,
					Collections.singletonMap("nm", it.get("_id").toString()),
					Boolean.class
			);
			if (!launchExists) {
				return null;
			}
			it.put("projectIds", new BasicDBObject());
			projects.toMap().entrySet().forEach(project -> updateDbObject(it, (Map.Entry) project));
			return it;
		};
	}

	private void updateDbObject(DBObject item, Map.Entry project) {
		if (project != null) {
			String projectName = (String) project.getKey();
			try {
				Long projectId = jdbcTemplate.queryForObject(SELECT_PROJECT_ID, Collections.singletonMap("name", projectName), Long.class);
				DBObject projectIds = (DBObject) item.get("projectIds");
				projectIds.put(String.valueOf(projectId), project.getValue());
			} catch (EmptyResultDataAccessException e) {
				LOGGER.warn(String.format("Project with name '%s' not found", projectName));
			}
		}
	}

	private static final String INSERT_LAUNCH_NUMBER = "INSERT INTO launch_number (project_id, launch_name, number) VALUES (:pr, :ln, :num)";

	@Bean
	public ItemWriter<DBObject> launchNumberWriter() {
		return it -> {
			SqlParameterSource[] sqlParameterSources = it.stream().flatMap(launchNumberItem -> {
				BasicDBObject projectIds = (BasicDBObject) launchNumberItem.get("projectIds");
				return projectIds.entrySet().stream().map(entry -> {
					MapSqlParameterSource parameterSource = new MapSqlParameterSource();
					parameterSource.addValue("pr", Long.valueOf(entry.getKey()));
					parameterSource.addValue("ln", launchNumberItem.get("_id").toString());
					parameterSource.addValue("num", entry.getValue());
					return parameterSource;
				}).collect(toList()).stream();
			}).toArray(SqlParameterSource[]::new);
			jdbcTemplate.batchUpdate(INSERT_LAUNCH_NUMBER, sqlParameterSources);
		};
	}

	@Bean(name = "migrateLaunchNumberStep")
	public Step migrateLaunchNumberStep() {
		return stepBuilderFactory.get("launchNumber").<DBObject, DBObject>chunk(30).reader(launchNumberReader())
				.processor(launchNumberProcessor())
				.writer(launchNumberWriter())
				.listener(chunkListener)
				.build();
	}

}
