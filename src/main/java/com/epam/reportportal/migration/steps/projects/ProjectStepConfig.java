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
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
@SuppressWarnings("unchecked")
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

	@Bean
	// Issue group have fixed ids in PostgreSQL
	public Map<String, Long> issueGroups() {
		Map<String, Long> groups = new HashMap<>(5);
		groups.put("TO_INVESTIGATE", 1L);
		groups.put("AUTOMATION_BUG", 2L);
		groups.put("PRODUCT_BUG", 3L);
		groups.put("NO_DEFECT", 4L);
		groups.put("SYSTEM_ISSUE", 5L);
		return groups;
	}

	@Bean
	// Default issue types have fixed ids in PostgreSQL
	public Map<String, Long> defaultIssueTypes() {
		Map<String, Long> issueTypes = new HashMap<>(5);
		issueTypes.put("ti001", 1L);
		issueTypes.put("ab001", 2L);
		issueTypes.put("pb001", 3L);
		issueTypes.put("nd001", 4L);
		issueTypes.put("si001", 5L);
		return issueTypes;
	}

	@Bean
	// Default attributes have fixed ids in PostgreSQL
	public Map<String, Long> defaultAttributes() {
		Map<String, Long> attributes = new HashMap<>(14);

		attributes.put("interruptJobTime", 1L);
		attributes.put("keepLogs", 3L);
		attributes.put("keepScreenshots", 4L);

		attributes.put("isAutoAnalyzerEnabled", 10L);
		attributes.put("analyzerMode", 11L);

		attributes.put("emailEnabled", 12L);
		attributes.put("from", 13L);
		return attributes;
	}
}
