package com.epam.reportportal.migration.steps.shareable.filter;

import com.epam.reportportal.migration.steps.shareable.ShareableWriter;
import com.google.common.collect.ImmutableMap;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class FilterWriter implements ItemWriter<DBObject> {

	public static final String INSERT_FILTER = "INSERT INTO filter (id, name, target, description) VALUES (:fid, :nm, :tar, :descr)";

	public static final String INSERT_FILTER_CONDITION =
			"INSERT INTO filter_condition (filter_id, condition, value, search_criteria, negative) "
					+ "VALUES (:fid, :cnd::FILTER_CONDITION_ENUM, :vl, :sc, :ng)";

	public static final String INSERT_FILTER_SORT = "INSERT INTO filter_sort (filter_id, field, direction) VALUES (:fid, :fld, :dir::SORT_DIRECTION_ENUM)";

	@Autowired
	private ShareableWriter shareableWriter;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private MongoTemplate mongoTemplate;

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
			storeIdsMapping(filter, entityId);
		});

		jdbcTemplate.batchUpdate(INSERT_FILTER_CONDITION, filterConditions.toArray(new SqlParameterSource[0]));
		jdbcTemplate.batchUpdate(INSERT_FILTER_SORT, filterSort.toArray(new SqlParameterSource[0]));
	}

	private void storeIdsMapping(DBObject filter, Long entityId) {
		mongoTemplate.upsert(Query.query(Criteria.where("_id").is(new ObjectId(filter.get("_id").toString()))),
				Update.update("postgresId", entityId),
				"filterMapping"
		);
	}

	private static Map<String, String> FIELDS_MAPPING = ImmutableMap.<String, String>builder().put("start_time", "startTime")
			.put("end_time", "endTime")
			.put("tags", "attributeValue")
			.build();

	private List<MapSqlParameterSource> conditionsSqlSources(DBObject filter, Long entityId) {
		return ((BasicDBList) ((DBObject) filter.get("filter")).get("filterConditions")).stream()
				.map(DBObject.class::cast)
				.map(condition -> {
					MapSqlParameterSource params = new MapSqlParameterSource();
					params.addValue("fid", entityId);
					params.addValue("cnd", condition.get("condition"));
					params.addValue("vl", condition.get("value"));
					params.addValue("sc",
							FIELDS_MAPPING.getOrDefault(condition.get("searchCriteria"), (String) condition.get("searchCriteria"))
					);
					params.addValue("ng", condition.get("negative"));
					return params;
				})
				.collect(Collectors.toList());
	}

	private List<MapSqlParameterSource> sortSqlSources(DBObject filter, Long entityId) {
		return ((BasicDBList) ((DBObject) filter.get("selectionOptions")).get("orders")).stream().map(DBObject.class::cast).map(order -> {
			MapSqlParameterSource params = new MapSqlParameterSource();
			params.addValue("fid", entityId);
			params.addValue("fld", FIELDS_MAPPING.getOrDefault(order.get("sortingColumnName"), (String) order.get("sortingColumnName")));
			String direction = ((boolean) order.get("isAsc")) ? "ASC" : "DESC";
			params.addValue("dir", direction);
			return params;
		}).collect(Collectors.toList());

	}

	private void writeFilter(DBObject filter, Long entityId) {
		DBObject info = (DBObject) filter.get("filter");

		String className = (String) ((DBObject) info.get("target")).get("className");
		String target = className.substring(className.lastIndexOf(".") + 1);

		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("fid", entityId);
		parameterSource.addValue("nm", filter.get("name"));
		parameterSource.addValue("tar", target);
		parameterSource.addValue("descr", filter.get("description"));

		jdbcTemplate.update(INSERT_FILTER, parameterSource);
	}
}
