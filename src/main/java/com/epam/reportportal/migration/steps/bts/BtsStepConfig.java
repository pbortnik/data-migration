package com.epam.reportportal.migration.steps.bts;

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
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.epam.reportportal.migration.steps.utils.MigrationUtils.SELECT_PROJECT_ID;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class BtsStepConfig {

	private final Logger LOGGER = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private BtsItemWriter btsItemWriter;

	@Autowired
	@Qualifier("chunkCountListener")
	private ChunkListener chunkCountListener;

	@Bean
	public Map<String, Long> btsIdMapping() {
		Map<String, Long> mapping = new HashMap<>(2);
		mapIntegrationType(mapping, "jira");
		mapIntegrationType(mapping, "rally");
		return mapping;
	}

	@Bean
	public ItemReader<DBObject> btsMongoReader() {
		if (btsIdMapping().isEmpty()) {
			return () -> null;
		}
		MongoItemReader<DBObject> itemReader = MigrationUtils.getMongoItemReader(mongoTemplate, "externalSystem");
		itemReader.setQuery("{'externalSystemType' : {$in : ['JIRA', 'RALLY']}}");
		return itemReader;
	}

	@Bean
	public ItemProcessor<DBObject, DBObject> btsItemProcessor() {
		return item -> {
			try {
				Long projectId = jdbcTemplate.queryForObject(SELECT_PROJECT_ID,
						Collections.singletonMap("name", item.get("projectRef")),
						Long.class
				);

				item.put("id", item.get("_id").toString());
				item.removeField("_id");
				item.removeField("_class");

				item.put("authType", item.get("externalSystemAuth"));
				item.removeField("externalSystemAuth");

				item.put("oauthAccessKey", item.get("accessKey"));
				item.removeField("accessKey");

				item.put("defectFormFields", item.get("fields"));
				item.removeField("fields");

				BasicDBObject params = new BasicDBObject("params", item);
				return new BasicDBObject("params", params).append("projectId", projectId)
						.append("integrationId", btsIdMapping().get(((String) item.get("externalSystemType")).toLowerCase()))
						.append("username", item.get("username"))

						.append("project", ((String) item.get("project")).toLowerCase());

			} catch (EmptyResultDataAccessException e) {
				LOGGER.debug(String.format("Project with name '%s' not found. Bts  is ignored", item.get("projectRef")));
				return null;
			}
		};
	}

	@Bean
	public Step migrateBtsStep() {
		return stepBuilderFactory.get("bts").<DBObject, DBObject>chunk(200).reader(btsMongoReader())
				.processor(btsItemProcessor())
				.writer(btsItemWriter)
				.listener(chunkCountListener)
				.build();
	}

	private void mapIntegrationType(Map<String, Long> mapping, String integrationName) {
		try {
			Long id = jdbcTemplate.queryForObject("SELECT id FROM integration_type WHERE name = :nm",
					Collections.singletonMap("nm", integrationName),
					Long.class
			);
			mapping.put(integrationName, id);
		} catch (EmptyResultDataAccessException e) {
			LOGGER.debug(String.format("Integration type with name '%s' not found.", integrationName));
		}
	}

}
