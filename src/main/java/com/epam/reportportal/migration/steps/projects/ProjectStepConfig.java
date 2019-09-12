package com.epam.reportportal.migration.steps.projects;

import com.mongodb.DBObject;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.HashMap;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class ProjectStepConfig {

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	@Qualifier("projectItemProcessor")
	private ItemProcessor projectItemProcessor;

	@Autowired
	@Qualifier("projectItemWriter")
	private ItemWriter projectItemWriter;

	@Bean
	public MongoItemReader<DBObject> projectMongoItemReader() {
		MongoItemReader<DBObject> mongoItemReader = new MongoItemReader<>();
		mongoItemReader.setTemplate(mongoTemplate);
		mongoItemReader.setTargetType(DBObject.class);
		mongoItemReader.setCollection("project");
		mongoItemReader.setSort(new HashMap<String, Sort.Direction>() {{
			put("_id", Sort.Direction.ASC);
		}});
		mongoItemReader.setQuery("{}");
		return mongoItemReader;
	}

	@Bean
	public Step migrateProjectsStep() {
		return stepBuilderFactory.get("project").<DBObject, DBObject>chunk(10).reader(projectMongoItemReader())
				.processor(projectItemProcessor)
				.writer(projectItemWriter)
				.build();
	}

}
