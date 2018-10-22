package com.epam.reportportal.servicecleaner;

import com.epam.ta.reportportal.config.MongodbConfiguration;
import com.epam.ta.reportportal.database.entity.Log;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.HashMap;

/**
 * @author Pavel Bortnik
 */
@Configuration
@EnableConfigurationProperties(MongodbConfiguration.MongoProperties.class)
@Import(MongodbConfiguration.class)
public class JobsConfiguration {

	@Autowired
	private JobBuilderFactory jobs;

	@Autowired
	private StepBuilderFactory steps;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Bean
	public MongoItemReader<Log> logMongoItemReader() {
		MongoItemReader<Log> mongoItemReader = new MongoItemReader<>();
		mongoItemReader.setTargetType(Log.class);
		mongoItemReader.setTemplate(mongoTemplate);
		mongoItemReader.setSort(new HashMap<String, Sort.Direction>() {{
			put("_id", Sort.Direction.DESC);
		}});
		mongoItemReader.setQuery("{}");
		return mongoItemReader;
	}

	@Bean
	public Step step() {
		return steps.get("step").<Log, Log>chunk(5).reader(logMongoItemReader())
				.processor(log -> log)
				.writer(items -> System.err.println(items.size()))
				.build();
	}

	@Bean
	public Job job() {
		return jobs.get("job").start(step()).build();
	}

}
