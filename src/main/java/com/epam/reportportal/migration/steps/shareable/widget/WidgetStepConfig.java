package com.epam.reportportal.migration.steps.shareable.widget;

import com.epam.reportportal.migration.steps.shareable.ShareableUtilService;
import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.google.common.collect.ImmutableMap;
import com.mongodb.DBObject;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class WidgetStepConfig {

	public static Map<String, String> CF_MAPPING = ImmutableMap.<String, String>builder().put("tags", "attributes")
			.put("start_time", "startTime")
			.put("end_time", "endTime")
			.put("last_modified", "lastModified")
			.build();
	public static Map<String, String> TYPE_MAPPING = ImmutableMap.<String, String>builder().put("old_line_chart", "oldLineChart")
			.put("investigated_trend", "investigatedTrend")
			.put("launch_statistics", "launchStatistics")
			.put("statistic_trend", "statisticTrend")
			.put("cases_trend", "casesTrend")
			.put("not_passed", "notPassed")
			.put("overall_statistics", "overallStatistics")
			.put("unique_bug_table", "uniqueBugTable")
			.put("bug_trend", "bugTrend")
			.put("launches_table", "launchesTable")
			.put("activity_stream", "activityStream")
			.put("launches_comparison_chart", "launchesComparisonChart")
			.put("launches_duration_chart", "launchesDurationChart")
			.put("most_failed_test_cases", "topTestCases")
			.put("flaky_test_cases", "topTestCases")
			.put("passing_rate_summary", "passingRateSummary")
			.put("passing_rate_per_launch", "passingRatePerLaunch")
			.put("product_status", "productStatus")
			.put("cumulative", "cumulative")
			.put("most_time_consuming", "mostTimeConsuming")
			.build();

	public static Map<String, String> WIDGET_OPTIONS_MAPPING = ImmutableMap.<String, String>builder().put("filterName", "launchNameFilter")
			.put("start_launch", "startLaunch")
			.put("finish_launch", "finishLaunch")
			.put("delete_launch", "deleteLaunch")
			.put("post_issue", "postIssue")
			.put("link_issue", "linkIssue")
			.put("unlink_issue", "unlinkIssue")
			.put("create_user", "createUser")
			.put("create_dashboard", "createDashboard")
			.put("update_dashboard", "updateDashboard")
			.put("delete_dashboard", "deleteDashboard")
			.put("create_widget", "createWidget")
			.put("update_widget", "updateWidget")
			.put("delete_widget", "deleteWidget")
			.put("create_filter", "createFilter")
			.put("update_filter", "updateFilter")
			.put("delete_filter", "deleteFilter")
			.put("create_bts", "createIntegration")
			.put("update_bts", "updateIntegration")
			.put("delete_bts", "deleteIntegration")
			.put("update_project", "updateProject")
			.put("update_analyzer", "updateAnalyzer")
			.put("generate_index", "generateIndex")
			.put("delete_index", "deleteIndex")
			.put("create_defect", "createDefect")
			.put("update_defect", "updateDefect")
			.put("delete_defect", "deleteDefect")
			.put("start_import", "startImport")
			.put("finish_import", "finishImport")
			.build();

	static Long ACL_CLASS;

	private static final int CHUNK_SIZE = 100;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private ChunkListener chunkCountListener;


	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private ItemWriter<DBObject> widgetWriter;

	@Autowired
	private ShareableUtilService shareableUtilService;

	@PostConstruct
	public void initialQueries() {
		try {
			ACL_CLASS = jdbcTemplate.queryForObject(
					"INSERT INTO acl_class (class, class_id_type) VALUES ('com.epam.ta.reportportal.entity.widget.Widget','java.lang.Long') RETURNING id",
					Long.class
			);
		} catch (Exception e) {
			ACL_CLASS = jdbcTemplate.queryForObject("SELECT id FROM acl_class WHERE class = 'com.epam.ta.reportportal.entity.widget.Widget'",
					Long.class
			);
		}
	}

	@Bean
	public MongoItemReader<DBObject> widgetItemReader() {
		MongoItemReader<DBObject> reader = MigrationUtils.getMongoItemReader(mongoTemplate, "widget");
		reader.setPageSize(CHUNK_SIZE);
		return reader;
	}

	@Bean("migrateWidgetStep")
	public Step migrateWidgetStep() {
		return stepBuilderFactory.get("widget").<DBObject, DBObject>chunk(CHUNK_SIZE).reader(widgetItemReader())
				.processor(widgetProcessor())
				.writer(widgetWriter)
				.listener(chunkCountListener)
				.build();
	}

	private ItemProcessor<DBObject, DBObject> widgetProcessor() {
		return widget -> {
			boolean processed = shareableUtilService.processShareableEntity(widget);
			if (!processed) {
				return null;
			}
			return widget;
		};
	}

}
