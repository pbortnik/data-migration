package com.epam.reportportal.migration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

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
	@Qualifier("levelItemsFlow")
	private List<Step> levelItemsFlow;

	@Autowired
	@Qualifier("migrateLogStep")
	private Step migrateLogStep;

	@Autowired
	@Qualifier("migrateFilterStep")
	private Step migrateFilterStep;

	@Autowired
	@Qualifier("migrateWidgetStep")
	private Step migrateWidgetStep;

	@Autowired
	private MigrationJobExecutionListener migrationJobExecutionListener;

	@Bean
	public Job job() {
		SimpleJobBuilder job = jobBuilderFactory.get("migrationJob")
				.listener(migrationJobExecutionListener)
				.start(migrateWidgetStep);
//				.next(migrateWidgetStep);
//				.next(migrateProjectsStep)
//				.next(migrateBtsStep)
//				.next(migrateLaunchStep)
//				.next(migrateLaunchNumberStep);
//		for (Step s : levelItemsFlow) {
//			job = job.next(s);
//		}
//		job.next(migrateLogStep);
//		job.next(migrateFilterStep);
		return job.build();
	}

}
