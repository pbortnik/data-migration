package com.epam.reportportal.migration.steps;

import com.github.benmanes.caffeine.cache.Cache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class StatisticsFieldsService {

	private static final String INSERT_STATISTICS_FIELD = "INSERT INTO statistics_field (name) VALUES (:nm) ON CONFLICT DO NOTHING RETURNING sf_id";

	private static final String SELECT_STATISTICS_FIELD = "SELECT sf_id FROM statistics_field WHERE name = :nm";

	public static final String TI_CUSTOM = "statistics$defects$to_investigate$%s";
	public static final String PB_CUSTOM = "statistics$defects$product_bug$%s";
	public static final String SI_CUSTOM = "statistics$defects$system_issue$%s";
	public static final String AB_CUSTOM = "statistics$defects$automation_bug$%s";
	public static final String ND_CUSTOM = "statistics$defects$no_defect$%s";

	@Autowired
	@Qualifier("statisticsFields")
	private Map<String, Long> statisticsFields;

	@Autowired
	private Cache<String, Long> customStatisticsFieldsCache;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public Long getStatisticsFieldId(String fieldRegex, String concreteField) {
		String fieldName = String.format(fieldRegex, concreteField.toLowerCase());
		Long defectFieldId = statisticsFields.get(fieldName);
		if (defectFieldId == null) {
			defectFieldId = customStatisticsFieldsCache.getIfPresent(fieldName);
			if (defectFieldId == null) {
				try {
					defectFieldId = jdbcTemplate.queryForObject(SELECT_STATISTICS_FIELD,
							Collections.singletonMap("nm", fieldName),
							Long.class
					);
				} catch (EmptyResultDataAccessException e) {
					defectFieldId = jdbcTemplate.queryForObject(INSERT_STATISTICS_FIELD,
							Collections.singletonMap("nm", fieldName),
							Long.class
					);
					customStatisticsFieldsCache.put(fieldName, defectFieldId);
				}
			}
		}
		return defectFieldId;
	}

	public Long getPredefinedStatisticsFieldId(String fieldName) {
		return statisticsFields.get(fieldName);
	}

}
