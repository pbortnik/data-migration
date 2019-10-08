package com.epam.reportportal.migration.steps.shareable.filter;

import com.epam.reportportal.migration.steps.shareable.ShareableWriter;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class FilterItemWriter implements ItemWriter<DBObject> {

	public static final String INSERT_FILTER = "INSERT INTO filter (id, name, target, description) VALUES (:fid, :nm, :tar, :descr)";

	public static final String INSERT_FILTER_CONDITION =
			"INSERT INTO filter_condition (filter_id, condition, value, search_criteria, negative) "
					+ "VALUES (:fid, :cnd::FILTER_CONDITION_ENUM, :vl, :sc, :ng)";

	public static final String INSERT_FILTER_SORT = "INSERT INTO filter_sort (filter_id, field, direction) VALUES (:fid, :fld, :dir::SORT_DIRECTION_ENUM)";

	@Autowired
	private ShareableWriter shareableWriter;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Override
	public void write(List<? extends DBObject> items) {
		List<MapSqlParameterSource> filterConditions = new ArrayList<>(items.size());
		List<MapSqlParameterSource> filterSort = new ArrayList<>(items.size());

		items.forEach(filter -> {
			Long entityId = shareableWriter.writeShareableEntity(filter);
			writeFilter(filter, entityId);
			filterConditions.addAll(conditionsSqlSources(filter, entityId));
			filterSort.addAll(sortSqlSources(filter, entityId));
			shareableWriter.writeAcl(filter, entityId, FilterStepConfig.ACL_CLASS);
		});

		jdbcTemplate.batchUpdate(INSERT_FILTER_CONDITION, filterConditions.stream().toArray(SqlParameterSource[]::new));
		jdbcTemplate.batchUpdate(INSERT_FILTER_SORT, filterSort.stream().toArray(SqlParameterSource[]::new));
	}

	private List<MapSqlParameterSource> conditionsSqlSources(DBObject filter, Long entityId) {
		return ((BasicDBList) ((DBObject) filter.get("filter")).get("filterConditions")).stream().map(condition -> {
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("fid", entityId);
			params.addValue("cnd", ((DBObject) condition).get("condition"));
			params.addValue("vl", ((DBObject) condition).get("value"));
			params.addValue("sc", ((DBObject) condition).get("searchCriteria"));
			params.addValue("ng", ((DBObject) condition).get("negative"));
			return params;
		}).collect(Collectors.toList());
	}

	private List<MapSqlParameterSource> sortSqlSources(DBObject filter, Long entityId) {
		return ((BasicDBList) ((DBObject) filter.get("selectionOptions")).get("orders")).stream().map(order -> {
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("fid", entityId);
			params.addValue("fld", ((DBObject) order).get("sortingColumnName"));
			String direction = ((boolean) ((DBObject) order).get("isAsc")) ? "ASC" : "DESC";
			params.addValue("dir", direction);
			return params;
		}).collect(Collectors.toList());

	}

	private void writeFilter(DBObject filter, Long entityId) {
		DBObject info = (DBObject) filter.get("filter");

		String className = (String) ((DBObject) info.get("target")).get("className");
		String target = className.substring(className.lastIndexOf("."), className.length() - 1);

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("fid", entityId);
		parameterSource.addValue("nm", filter.get("name"));
		parameterSource.addValue("tar", target);
		parameterSource.addValue("descr", filter.get("description"));

		jdbcTemplate.update(INSERT_FILTER, parameterSource);
	}
}
