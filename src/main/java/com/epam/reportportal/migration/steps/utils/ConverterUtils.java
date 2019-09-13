package com.epam.reportportal.migration.steps.utils;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
public class ConverterUtils {

	public static Timestamp toUtc(Date date) {
		return Timestamp.valueOf(LocalDateTime.ofInstant(date.toInstant(), ZoneOffset.UTC));
	}

}
