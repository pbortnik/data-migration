package com.epam.reportportal.migration.steps;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import static com.epam.reportportal.migration.steps.StatisticsFieldsService.*;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class CommonItemWriter {

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private StatisticsFieldsService fieldsService;

	public void writeTags(BasicDBList tags, String query, Long id) {
		if (CollectionUtils.isEmpty(tags)) {
			return;
		}
		jdbcTemplate.batchUpdate(query, tags.stream().map(it -> {
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("val", it);
			params.addValue("id", id);
			return params;
		}).toArray(SqlParameterSource[]::new));
	}

	public void writeStatistics(DBObject statistics, String query, Long id) {
		DBObject executions = (DBObject) statistics.get("executionCounter");

		SqlParameterSource[] executionParams = executions.keySet().stream().map(it -> {
			Integer counter = (Integer) executions.get(it);
			return populateValues(id, fieldsService.getPredefinedStatisticsFieldId(it), counter);
		}).filter(Objects::nonNull).toArray(SqlParameterSource[]::new);

		DBObject defects = (DBObject) statistics.get("issueCounter");

		SqlParameterSource[] params = Stream.of(
				executionParams,
				prepareParams("automationBug", AB_CUSTOM, id, defects),
				prepareParams("productBug", PB_CUSTOM, id, defects),
				prepareParams("systemIssue", SI_CUSTOM, id, defects),
				prepareParams("toInvestigate", TI_CUSTOM, id, defects),
				prepareParams("noDefect", ND_CUSTOM, id, defects)
		).flatMap(Arrays::stream).toArray(SqlParameterSource[]::new);

		jdbcTemplate.batchUpdate(query, params);
	}

	private SqlParameterSource[] prepareParams(String type, String fieldRegex, Long id, DBObject defects) {
		DBObject bugType = (DBObject) defects.get(type);
		return bugType.keySet().stream().map(concreteField -> {
			Long defectFieldId = fieldsService.getStatisticsFieldId(fieldRegex, concreteField);
			Integer counter = (Integer) bugType.get(concreteField);
			return populateValues(id, defectFieldId, counter);
		}).filter(Objects::nonNull).toArray(SqlParameterSource[]::new);
	}

	private MapSqlParameterSource populateValues(Long id, Long fieldId, Integer counter) {
		if (counter > 0) {
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("ct", counter);
			params.addValue("id", id);
			params.addValue("sfi", fieldId);
			return params;
		}
		return null;
	}

	public void writeParams(BasicDBList parameters, String insertItemParameters, Long itemId) {
		if (CollectionUtils.isEmpty(parameters)) {
			return;
		}
		jdbcTemplate.batchUpdate(insertItemParameters, parameters.stream().map(it -> {
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("key", ((DBObject) it).get("key"));
			params.addValue("val", ((DBObject) it).get("val"));
			params.addValue("id", itemId);
			return params;
		}).toArray(SqlParameterSource[]::new));
	}
}
