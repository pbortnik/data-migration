package com.epam.reportportal.migration.steps.bts;

import com.mongodb.DBObject;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Component("btsItemWriter")
public class BtsItemWriter implements ItemWriter<DBObject> {

	@Autowired
	private NamedParameterJdbcTemplate jdbcTemplate;

	@Autowired
	private BtsSqlParameterSourceProvider paramProvider;

	public static final String INSERT_BTS = "INSERT INTO integration (project_id, type, enabled, params, creator, name) VALUES (:pr, :tp, :en, :params::JSONB, :cr, :nm)";

	@Override
	public void write(List<? extends DBObject> items) {
		SqlParameterSource[] parameters = items.stream()
				.map(it -> paramProvider.createSqlParameterSource(it))
				.toArray(SqlParameterSource[]::new);
		jdbcTemplate.batchUpdate(INSERT_BTS, parameters);
	}
}
