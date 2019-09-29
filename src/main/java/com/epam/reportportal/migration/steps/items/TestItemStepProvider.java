package com.epam.reportportal.migration.steps.items;

import org.springframework.batch.core.Step;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component
public class TestItemStepProvider {

	@Autowired
	private Function<Integer, Step> myPrototypeFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Bean
	public List<Step> levelItemsFlow() {
		int pathSize = 0;
		while (true) {
			boolean exists = mongoTemplate.exists(Query.query(Criteria.where("path").size(pathSize)), "testItem");
			if (!exists) {
				pathSize--;
				break;
			}
			pathSize++;
		}

		List<Step> steps = new LinkedList<>();
		for (Integer i = 0; i <= pathSize; i++) {
			Step step = myPrototypeFactory.apply(i);
			steps.add(step);
		}
		return steps;
	}
}
