package com.epam.reportportal.migration.steps.settings;

import com.epam.reportportal.migration.steps.utils.MigrationUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.MongoItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author <a href="mailto:pavel_bortnik@epam.com">Pavel Bortnik</a>
 */
@Configuration
public class SettingsConfig {

	private static final String INSERT_SERVER_SETTINGS = "INSERT INTO server_settings (key, value) VALUES (?,?)";
	private static final String INSERT_INTEGRATION = "INSERT INTO integration (name, type, enabled, params, creator) VALUES (?,?,?,?,?)";
	private static final Long EMAIL_INTEGRAION_ID = 2L;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private MongoTemplate mongoTemplate;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Bean
	public MongoItemReader<DBObject> settingsReader() {
		return MigrationUtils.getMongoItemReader(mongoTemplate, "serverSettings");
	}

	@Bean
	public ItemProcessor<DBObject, DBObject> settingsProcessor() {
		return item -> item;
	}

	@Bean
	public ItemWriter<? super DBObject> settingsWriter() {
		return items -> {
			items.forEach(item -> {
				writeEmailIntegration(item);
				writeServerSettings(item);
				writeGithubSettings(item);
				writeSaml(item);
			});
		};
	}

	private void writeSaml(DBObject item) {
		DBObject samlProviderDetails = (DBObject) item.get("samlProviderDetails");
		if (samlProviderDetails != null) {
			samlProviderDetails.toMap().keySet().stream().forEach(saml -> {
				DBObject provider = (DBObject) samlProviderDetails.get((String) saml);
				jdbcTemplate.update(
						"INSERT INTO saml_provider_details (idp_name, idp_metadata_url, idp_name_id, idp_alias, idp_url, "
								+ "full_name_attribute_id, first_name_attribute_id, last_name_attribute_id, email_attribute_id, enabled) "
								+ "VALUES (?,?,?,?,?,?,?,?,?,?)",
						provider.get("idpName"),
						provider.get("idpMetadata"),
						provider.get("idpNameId"),
						provider.get("idpAlias"),
						provider.get("idpUrl"),
						provider.get("firstNameAttributeId") + " " + provider.get("lastNameAttributeId"),
						provider.get("firstNameAttributeId"),
						provider.get("lastNameAttributeId"),
						provider.get("emailAttributeId"),
						provider.get("enabled")
				);
			});
		}
	}

	private void writeGithubSettings(DBObject item) {
		DBObject oAuth2LoginDetails = (DBObject) item.get("oAuth2LoginDetails");
		if (oAuth2LoginDetails != null) {
			DBObject github = (DBObject) oAuth2LoginDetails.get("github");
			if (github != null) {
				jdbcTemplate.update(
						"INSERT INTO oauth_registration (id, client_id, client_secret, client_auth_method, auth_grant_type, "
								+ "redirect_uri_template, authorization_uri, token_uri, user_info_endpoint_uri, user_info_endpoint_name_attr, "
								+ "jwk_set_uri, client_name) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
						"github",
						github.get("clientId"),
						github.get("clientSecret"),
						github.get("clientAuthenticationScheme"),
						github.get("grantType"),
						"{baseUrl}/{action}/oauth2/code/{registrationId}",
						github.get("userAuthorizationUri"),
						github.get("accessTokenUri"),
						"https://api.github.com/user",
						"id",
						null,
						"github"
				);

				List<Object[]> params = ((BasicDBList) github.get("scope")).stream()
						.map(scope -> new Object[] { "github", scope })
						.collect(Collectors.toList());
				jdbcTemplate.update("INSERT INTO oauth_registration_scope (oauth_registration_fk, scope) VALUES (?, ?)", params);
			}
		}
	}

	private void writeServerSettings(DBObject item) {
		Boolean analytics = (Boolean) ((DBObject) item.get("analyticsDetails")).get("all");
		List<Object[]> objects = new ArrayList<>();
		objects.add(new Object[] { "server.analytics.all", analytics });
		objects.add(new Object[] { "server.details.instance", item.get("instanceId") });
		jdbcTemplate.update(INSERT_SERVER_SETTINGS, objects);
	}

	private void writeEmailIntegration(DBObject dbObject) {
		DBObject serverEmailDetails = (DBObject) dbObject.get("serverEmailDetails");
		if (serverEmailDetails != null) {
			BasicDBObject params = new BasicDBObject().append("params", serverEmailDetails);
			jdbcTemplate.update(
					INSERT_INTEGRATION,
					"email server",
					EMAIL_INTEGRAION_ID,
					serverEmailDetails.get("enabled"),
					params.toString(),
					serverEmailDetails.get("username")
			);
		}
	}

	@Bean
	public Step migratePreferencesStep() {
		return stepBuilderFactory.get("settings").<DBObject, DBObject>chunk(1).reader(settingsReader())
				.processor(settingsProcessor())
				.writer(settingsWriter())
				.build();
	}

}
