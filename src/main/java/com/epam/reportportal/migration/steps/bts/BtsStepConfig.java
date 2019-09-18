package com.epam.reportportal.migration.steps.bts;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
@SuppressWarnings("unchecked")
public class BtsStepConfig {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private BtsItemWriter btsItemWriter;

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
				Long projectId = jdbcTemplate.queryForObject("SELECT id FROM project WHERE project.name = :name",
						Collections.singletonMap("name", item.get("projectRef")),
						Long.class
				);

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

			} catch (Exception e) {
				System.out.println("Project with name " + item.get("projectRef") + " not found.");
				return null;
			}
		};
	}

	@Bean
	public Step migrateBtsStep() {
		return stepBuilderFactory.get("bts").<DBObject, DBObject>chunk(10).reader(btsMongoReader())
				.processor(btsItemProcessor())
				.writer(btsItemWriter)
				.build();
	}

	private void mapIntegrationType(Map<String, Long> mapping, String integrationName) {
		try {
			Long id = jdbcTemplate.queryForObject("SELECT id FROM integration_type WHERE name = :nm",
					Collections.singletonMap("nm", integrationName),
					Long.class
			);
			mapping.put(integrationName, id);
		} catch (Exception e) {
			System.out.println(integrationName + " not found");
		}
	}

}
