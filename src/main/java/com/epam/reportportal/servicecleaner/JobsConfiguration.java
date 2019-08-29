package com.epam.reportportal.servicecleaner;

import com.mongodb.DBObject;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;

import javax.sql.DataSource;
import java.util.HashMap;

/**
 * @author Pavel Bortnik
 */
@Configuration
@EnableBatchProcessing
public class JobsConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private DataSource dataSource;

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
	public ItemWriter<DBObject> writer() {
		return it -> System.out.println(it.size());
	}

	@Bean
	public Step step() {
		return stepBuilderFactory.get("step").<DBObject, DBObject>chunk(10).reader(userMongoItemReader())
				.processor(userItemProcessor())
				.writer(writer())
				.build();
	}

	@Bean
	public Job job() {
		return jobBuilderFactory.get("job").flow(step()).end().build();
	}

}
