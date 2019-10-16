package com.epam.reportportal.migration.steps.shareable.dashboard;

import com.epam.reportportal.migration.steps.shareable.ShareableWriter;
import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class DashboardWriter implements ItemWriter<DBObject> {

	private static final String INSERT_DASHBOARD = "INSERT INTO dashboard (id, name, description, creation_date) VALUES (?,?,?,?)";

	private static final String INSERT_WIDGET_DASHBOARD =
			"INSERT INTO dashboard_widget (dashboard_id, widget_id, widget_name, widget_owner, share, "
					+ "widget_type, widget_width, widget_height, widget_position_x, widget_position_y, is_created_on) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)";

	@Autowired
	private ShareableWriter shareableWriter;

	@Autowired
	private JdbcTemplate template;

	@Override
	public void write(List<? extends DBObject> items) {
		List<Object[]> dashboardSqlParams = new ArrayList<>(items.size());
		List<Object[]> dashboardWidgetParams = new ArrayList<>(items.size());
		items.forEach(dashboard -> {
			Long dashboardId = shareableWriter.writeShareableEntity(dashboard);
			shareableWriter.writeAcl(dashboard, dashboardId, DashboardStepConfig.ACL_CLASS);
			dashboardSqlParams.add(prepareSqlParams(dashboard, dashboardId));
			dashboardWidgetParams.addAll(prepareWidgetsSqlParams((BasicDBList) dashboard.get("widgets"), dashboardId));
		});
		template.batchUpdate(INSERT_DASHBOARD, dashboardSqlParams);
		template.batchUpdate(INSERT_WIDGET_DASHBOARD, dashboardWidgetParams);
	}

	private List<Object[]> prepareWidgetsSqlParams(BasicDBList widgets, Long dashboardId) {
		List<Object[]> params = new ArrayList<>(widgets.size());
		widgets.stream().map(DBObject.class::cast).forEach(widget -> {
			params.add(new Object[] { dashboardId, widget.get("postgresId"), widget.get("name"), widget.get("owner"), widget.get("shared"),
					widget.get("type"), ((BasicDBList) widget.get("widgetSize")).get(0), ((BasicDBList) widget.get("widgetSize")).get(1),
					((BasicDBList) widget.get("widgetPosition")).get(0), ((BasicDBList) widget.get("widgetPosition")).get(1) });
		});
		return params;
	}

	private Object[] prepareSqlParams(DBObject dashboard, Long id) {
		return new Object[] { id, dashboard.get("name"), dashboard.get("description"),
				MigrationUtils.toUtc((Date) dashboard.get("creationDate")) };
	}
}
