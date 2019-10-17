package com.epam.reportportal.migration.steps.shareable.widget;

import com.epam.reportportal.migration.steps.shareable.ShareableWriter;
import com.epam.reportportal.migration.steps.utils.CacheableDataService;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.epam.reportportal.migration.steps.shareable.widget.WidgetStepConfig.*;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@SuppressWarnings("ALL")
@Component
public class WidgetWriter implements ItemWriter<DBObject> {

	private static final String INSERT_WIDGET = "INSERT INTO widget (id, name, description, widget_type, items_count, widget_options) "
			+ "VALUES (:id, :nm, :descr, :wt, :ic, :opt::JSONB)";

	private static final String INSERT_CONTENT_FIELDS = "INSERT INTO content_field (id, field) VALUES (?, ?)";

	private static final String INSERT_WIDGET_FILTER = "INSERT INTO widget_filter (widget_id, filter_id) VALUES (?, ?)";

	@Autowired
	private JdbcTemplate template;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private ShareableWriter shareableWriter;

	@Autowired
	private CacheableDataService cacheableDataService;

	@Override
	public void write(List<? extends DBObject> items) {
		Map<String, Long> filterMapping = cacheableDataService.loadFilterIdsMapping(items.stream()
				.map(it -> it.get("applyingFilterId"))
				.filter(Objects::nonNull)
				.map(String.class::cast)
				.filter(it -> ObjectId.isValid(it))
				.map(id -> new ObjectId((String) id))
				.collect(Collectors.toSet()));

		List<Object[]> widgetFilter = new ArrayList<>(items.size());
		List<Object[]> contentFields = new ArrayList<>();

		items.forEach(widget -> {
			Long widgetId = write(widget);
			contentFields.addAll(contentFieldsSqlSources(widgetId,
					(BasicDBList) ((DBObject) widget.get("contentOptions")).get("contentFields")
			));
			if (null != filterMapping.get(widget.get("applyingFilterId"))) {
				widgetFilter.add(new Object[] { widgetId, filterMapping.get(widget.get("applyingFilterId")) });
			}
			storeIdsMapping(widget, widgetId);
		});

		template.batchUpdate(INSERT_WIDGET_FILTER, widgetFilter);
		template.batchUpdate(INSERT_CONTENT_FIELDS, contentFields);
	}

	private Long write(DBObject widget) {
		Long widgetId = shareableWriter.writeShareableEntity(widget);
		insertWidget(widget, widgetId);
		shareableWriter.writeAcl(widget, widgetId, WidgetStepConfig.ACL_CLASS);
		return widgetId;
	}

	private void insertWidget(DBObject widget, Long widgetId) {
		DBObject contentOptions = (DBObject) widget.get("contentOptions");
		MapSqlParameterSource parameterSource = new MapSqlParameterSource();
		parameterSource.addValue("id", widgetId);
		parameterSource.addValue("nm", widget.get("name"));
		parameterSource.addValue("descr", widget.get("description"));
		parameterSource.addValue("wt", TYPE_MAPPING.get(contentOptions.get("gadgetType")));
		parameterSource.addValue("ic", contentOptions.get("itemsCount"));

		String widgetOptions = processWidgetOptions(contentOptions);
		parameterSource.addValue("opt", widgetOptions);

		jdbcTemplate.update(INSERT_WIDGET, parameterSource);

	}

	private String processWidgetOptions(DBObject contentOptions) {
		DBObject widgetOptions = (DBObject) contentOptions.get("widgetOptions");
		switch ((String) contentOptions.get("gadgetType")) {

			case "activity_stream":
				processActivityOptions(widgetOptions);
				break;

			case "investigated_trend":
			case "cases_trend":
				widgetOptions = processTimeline(widgetOptions);
				break;

			case "flaky_test_cases":
			case "most_failed_test_cases":
				widgetOptions = processFlaky(widgetOptions);
				break;

			case "launches_duration_chart":
				widgetOptions = processLatest(widgetOptions);
				break;
			case "overall_statistics":
				widgetOptions = processLatest(widgetOptions);
				widgetOptions = processOverall(widgetOptions);
				break;
			case "passing_rate_per_launch":
				widgetOptions = processLaunchFilterName(widgetOptions);
				widgetOptions = processView(widgetOptions);
				break;
			case "passing_rate_summary":
				widgetOptions = processView(widgetOptions);
				break;
			case "statistic_trend":
				widgetOptions = processTimeline(widgetOptions);
				widgetOptions = processStatsTrend(widgetOptions);
				break;
			case "unique_bug_table":
				widgetOptions = new BasicDBObject().append("latest", false);
				break;
			default:
				widgetOptions = new BasicDBObject();

		}
		return new BasicDBObject("options", widgetOptions).toString();
	}

	private DBObject processStatsTrend(DBObject widgetOptions) {
		BasicDBList basicDBList = (BasicDBList) widgetOptions.get("viewMode");
		if (!CollectionUtils.isEmpty(basicDBList)) {
			String viewMode = (String) basicDBList.get(0);
			widgetOptions.put("viewMode", "bar");
			if (viewMode.equalsIgnoreCase("areaChartMode")) {
				widgetOptions.put("viewMode", "area-spline");
			}
		} else {
			widgetOptions.put("viewMode", "bar");
		}
		widgetOptions.put("zoom", false);
		return widgetOptions;
	}

	private DBObject processView(DBObject widgetOptions) {
		if (widgetOptions.get("viewMode") != null) {
			String viewMode = (String) ((BasicDBList) widgetOptions.get("viewMode")).get(0);
			if (viewMode.equalsIgnoreCase("barMode")) {
				widgetOptions.put("viewMode", "bar");
			} else if (viewMode.equalsIgnoreCase("pieChartMode")) {
				widgetOptions.put("viewMode", "pie");
			}
			return widgetOptions;
		}
		widgetOptions.put("viewMode", "bar");
		return widgetOptions;
	}

	private DBObject processLaunchFilterName(DBObject widgetOptions) {
		if (widgetOptions != null) {
			BasicDBList launchNameFilter = (BasicDBList) widgetOptions.get("launchNameFilter");
			if (!CollectionUtils.isEmpty(launchNameFilter)) {
				widgetOptions.put("launchNameFilter", launchNameFilter.get(0));
			}
			return widgetOptions;
		}
		return new BasicDBObject();
	}

	private DBObject processOverall(DBObject widgetOptions) {
		BasicDBList viewMode = (BasicDBList) widgetOptions.get("viewMode");
		if (!CollectionUtils.isEmpty(viewMode)) {
			widgetOptions.put("viewMode", viewMode.get(0));
		} else {
			widgetOptions.put("viewMode", "panel");
		}
		return widgetOptions;
	}

	private DBObject processLatest(DBObject widgetOptions) {
		if (widgetOptions != null) {
			Object latest = widgetOptions.get("latest");
			if (latest != null) {
				widgetOptions.put("latest", true);
				return widgetOptions;
			}
		}
		return new BasicDBObject("latest", false);
	}

	private DBObject processFlaky(DBObject widgetOptions) {
		if (widgetOptions != null) {
			if (widgetOptions.get("include_methods") != null) {
				widgetOptions.put("includeMethods", true);
				widgetOptions.removeField("include_methods");
			} else {
				widgetOptions.put("includeMethods", false);
			}
			if (widgetOptions.get("launchNameFilter") != null) {
				widgetOptions.put("launchNameFilter", ((BasicDBList) widgetOptions.get("launchNameFilter")).get(0));
			}
			return widgetOptions;
		}
		return new BasicDBObject();
	}

	private DBObject processTimeline(DBObject widgetOptions) {
		if (widgetOptions != null) {
			BasicDBList timeline = (BasicDBList) widgetOptions.get("timeline");
			if (!CollectionUtils.isEmpty(timeline)) {
				String res = timeline.get(0).toString().toLowerCase();
				widgetOptions.put("timeline", res);
			}
			return widgetOptions;
		}
		return new BasicDBObject();
	}

	private void processActivityOptions(DBObject widgetOptions) {
		if (widgetOptions.get("userRef") != null) {
			widgetOptions.put("userRef",
					((BasicDBList) widgetOptions.get("userRef")).stream().map(String.class::cast).collect(Collectors.joining(","))
			);
		}
		widgetOptions.put("actionType",
				((BasicDBList) widgetOptions.get("actionType")).stream()
						.map(String.class::cast)
						.map(it -> WIDGET_OPTIONS_MAPPING.get(it))
						.collect(Collectors.toList())
		);
	}

	private List<Object[]> contentFieldsSqlSources(Long widgetId, BasicDBList contentFields) {
		if (contentFields != null) {
			return contentFields.stream().map(cf -> CF_MAPPING.getOrDefault(cf, (String) cf)).map(cf -> {
				if (cf.startsWith("statistics")) {
					return cf.toLowerCase();
				}
				return cf;
			}).map(cf -> new Object[] { widgetId, cf }).collect(Collectors.toList());
		}
		return Collections.emptyList();
	}

	private void storeIdsMapping(DBObject widget, Long entityId) {
		mongoTemplate.upsert(Query.query(Criteria.where("_id").is(new ObjectId(widget.get("_id").toString()))),
				Update.update("postgresId", entityId)
						.set("name", widget.get("name"))
						.set("share", widget.get("shared"))
						.set("owner", ((DBObject) widget.get("acl")).get("ownerUserId"))
						.set("type", TYPE_MAPPING.get(((DBObject) widget.get("contentOptions")).get("gadgetType"))),
				"widgetMapping"
		);
	}

}
