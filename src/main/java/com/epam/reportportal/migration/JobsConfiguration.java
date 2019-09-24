package com.epam.reportportal.migration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Pavel Bortnik
 */
@Configuration
@EnableBatchProcessing
public class JobsConfiguration {

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	@Qualifier("migrateUsersStep")
	private Step migrateUserStep;

	@Autowired
	@Qualifier("migrateProjectsStep")
	private Step migrateProjectsStep;

	@Autowired
	@Qualifier("migrateBtsStep")
	private Step migrateBtsStep;

	@Autowired
	@Qualifier("migrateLaunchStep")
	private Step migrateLaunchStep;

	@Autowired
	@Qualifier("migrateLaunchNumberStep")
	private Step migrateLaunchNumberStep;

	@Autowired
	@Qualifier("migrateTestItemStep")
	private Step migrateTestItemStep;

	@Bean
	public Job job() {
		return jobBuilderFactory.get("job")
				.flow(migrateTestItemStep)
//				.flow(migrateUserStep)
//				.next(migrateProjectsStep)
//				.next(migrateBtsStep)
//				.next(migrateLaunchStep)
//				.next(migrateLaunchNumberStep)
				.end()
				.build();
	}

}
