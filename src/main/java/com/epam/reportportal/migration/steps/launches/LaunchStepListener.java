package com.epam.reportportal.migration.steps.launches;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class LaunchStepListener implements StepExecutionListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(LaunchStepListener.class);

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Override
	public void beforeStep(StepExecution stepExecution) {
		jdbcTemplate.execute("ALTER TABLE launch DISABLE TRIGGER ALL;");
		LOGGER.debug("Triggers disabled for launch table");
	}

	@Override
	public ExitStatus afterStep(StepExecution stepExecution) {
		jdbcTemplate.execute("ALTER TABLE launch ENABLE TRIGGER ALL;");
		LOGGER.debug("Triggers enabled for the launch table");
		return ExitStatus.COMPLETED;
	}
}
