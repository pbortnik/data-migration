package com.epam.reportportal.migration;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class MigrationJobExecutionListener implements JobExecutionListener {

	@Autowired
	@Qualifier("threadPoolTaskExecutor")
	private ThreadPoolTaskExecutor taskExecutor;

	@Override
	public void beforeJob(JobExecution jobExecution) {

	}

	@Override
	public void afterJob(JobExecution jobExecution) {
		taskExecutor.shutdown();
	}
}
