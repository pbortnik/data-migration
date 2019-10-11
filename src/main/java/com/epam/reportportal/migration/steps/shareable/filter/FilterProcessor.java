package com.epam.reportportal.migration.steps.shareable.filter;

import com.epam.reportportal.migration.steps.shareable.ShareableUtilService;
import com.google.common.collect.ImmutableMap;
import com.mongodb.DBObject;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class FilterProcessor implements ItemProcessor<DBObject, DBObject> {

	private Map<String, String> FIELDS_MAPPING = ImmutableMap.<String, String>builder()
			//ACTIVITY
			.put("actionType", "action")
			.put("userRef", "user")
			.put("name", "objectName")
			.put("loggedObjectRef", "objectId")
			.put("last_modified", "lastModified")
			.put("projectRef", "name")
			.put("objectType", "entity")
			//UserFilter
			.put("name", "name")
			.put("projectName", "")
			//Log
			.put("level", "level")
			.put("binary_content$id", "binaryContent")
			.put("binary_content$thumbnail_id", "binaryContent")
			.put("id", "id")
			.put("time", "logTime")
			.put("message", "message")
			.put("last_modified", "lastModified")
			.put("binary_content", "binaryContent")
			//TESTitem
			.put("parent", "")
			.put("statistics$defects$no_defect", "")
			.put("statistics$defects$to_investigate", "")
			.put("issue$externalSystemIssues$submitDate", "")
			.put("statistics$executions$passed", "")
			.put("issue$externalSystemIssues$submitter", "")
			.put("description", "")
			.put("issue$ignore_analyzer", "")
			.put("statistics$defects$product_bug", "")
			.put("type", "")
			.put("path", "path")
			.put("issue$auto_analyzed", "")
			.put("statistics$defects", "")
			.put("issue$externalSystemIssues", "")
			.put("id", "")
			.put("issue$issue_comment", "")
			.put("last_modified", "")
			.put("statistics$defects$system_issue", "")
			.put("statistics$defects$automation_bug", "")
			.put("issue", "")
			.put("issue$issue_type", "")
			.put("parameters$key", "")
			.put("parameters$value", "")
			.put("end_time", "")
			.put("statistics$executions", "")
			.put("has_childs", "")
			.put("launch", "")
			.put("statistics$executions$failed", "")
			.put("issue$externalSystemIssues$ticket_id", "")
			.put("tags", "")
			.put("statistics$executions$total", "")
			.put("start_time", "")
			.put("issue$externalSystemIssues$system_id", "")
			.put("statistics$executions$skipped", "")
			.put("name", "")
			.put("parameters", "")
			.put("uniqueId", "")
			.put("status", "")
			.put("statistics", "")
			//USER
			.put("expired", "")
			.put("name", "")
			.put("login", "")
			.put("type", "")
			.put("email", "")
			//Execution counter
			.put("total", "")
			.put("passed", "")
			.put("failed", "")
			.put("skipped", "")
			//PROJECT
			.put("configuration", "")
			.put("name", "")
			.put("configuration$entryType", "")
			.put("creationDate", "")
			.put("users", "")
			//Launch
			.put("statistics$defects$system_issue", "")
			.put("statistics$defects$automation_bug", "")
			.put("statistics$defects$no_defect", "")
			.put("statistics$defects$to_investigate", "")
			.put("statistics$executions$passed", "")
			.put("end_time", "")
			.put("project", "")
			.put("description", "")
			.put("statistics$executions", "")
			.put("statistics$defects$product_bug", "")
			.put("statistics$executions$failed", "")
			.put("tags", "")
			.put("statistics$executions$total", "")
			.put("mode", "")
			.put("start_time", "")
			.put("number", "")
			.put("statistics$executions$skipped", "")
			.put("statistics$defects", "")
			.put("name", "")
			.put("user", "")
			.put("last_modified", "")
			.put("status", "")
			.put("statistics", "")
			//Issue
			.put("issue_comment", "")
			.put("externalSystemIssues$submitter", "")
			.put("externalSystemIssues$ticket_id", "")
			.put("ignore_analyzer", "")
			.put("issue_type", "")
			.put("externalSystemIssues$system_id", "")
			.put("externalSystemIssues", "")
			.put("externalSystemIssues$submitDate", "")
			.put("auto_analyzed", "")
			//Statistics
			.put("executions$failed", "")
			.put("defects$no_defect", "")
			.put("executions$passed", "")
			.put("executions", "")
			.put("defects$to_investigate", "")
			.put("defects$product_bug", "")
			.put("defects", "")
			.put("executions$total", "")
			.put("executions$skipped", "")
			.put("defects$system_issue", "")
			.put("defects$automation_bug", "")
			//defects
			.put("no_defect", "")
			.put("system_issue", "")
			.put("product_bug", "")
			.put("to_investigate", "")
			.put("automation_bug", "")
			.build();

	@Autowired
	private ShareableUtilService shareableUtilService;

	@Override
	public DBObject process(DBObject item) {
		boolean processed = shareableUtilService.processShareableEntity(item);
		if (!processed) {
			return null;
		}
		return item;
	}
}
