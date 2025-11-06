package eu.europeana.api.analytics.config;

import com.databox.sdk.Databox;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import eu.europeana.api.analytics.service.ClientsServiceConnection;
import eu.europeana.api.analytics.service.ElasticSearchConnection;
import eu.europeana.api.commons.auth.AuthenticationBuilder;
import eu.europeana.api.commons.auth.AuthenticationConfig;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static eu.europeana.api.analytics.utils.Constants.DATABOX;
import static eu.europeana.api.analytics.utils.Constants.ELASTIC_SEARCH_CONNECTION;
import static eu.europeana.api.analytics.utils.Constants.REGISTERED_CLIENT_CONNECTION;

/**
 * Configuration class of Analytics api
 * @author srishti singh
 */
@Configuration
public class AnalyticsApiConfig {

    @Value("${set.api.stats.url}")
    private String setApiStatsUrl;

    @Value("${apikey.stats.url}")
    private String userStatsUrl;

    @Value("${entity.stats.url}")
    private String entityStatsUrl;

    @Value("${search.api.stats.url}")
    private String searchApiUrl;

    @Value("${keycloak.registered.clients.url}")
    private String registeredClientsUrl;

    @Value("${token_endpoint}")
    private String tokenEndpoint;

    @Value("${grant_params}")
    private String grantParams;

    @Value("${databox.token}")
    private String databoxToken;

    @Value("${elastic.search.url}")
    private String elasticSearchUrl;

    @Value("${es.request.body.file.name}")
    private String esBodyFileName;

    @Value("${calls.per.day}")
    private int callsPerDay;

    @Value("${active.days}")
    private int activeDays;

    @Value("${apikey_dates.csv.file.location}")
    private String apiKeyAndDatesCsvFile;

    @Value("${apikey.summary.file.location}")
    private String apiKeySummaryFile;

    @Value("${monthly.schedule: 1}")
    private int dayOfMonth;

    public String getSetApiStatsUrl() {
        return setApiStatsUrl;
    }

    public String getUserStatsUrl() {
        return userStatsUrl;
    }

    public String getEntityStatsUrl() {
        return entityStatsUrl;
    }

    public String getSearchApiUrl() {
        return searchApiUrl;
    }

    public String getRegisteredClientsUrl() {
        return registeredClientsUrl;
    }

    public String getDataboxToken() {
        return databoxToken;
    }

    public String getElasticSearchUrl() {
        return elasticSearchUrl;
    }

    public int getCallsPerDay() {
        return callsPerDay;
    }

    public int getActiveDays() {
        return activeDays;
    }

    private String getEsBodyFileName() {
        return "/" + esBodyFileName;
    }

    public File getApiKeyAndDatesCsvFile() {
        return StringUtils.isEmpty(apiKeyAndDatesCsvFile) ? null : new File(apiKeyAndDatesCsvFile);
    }

    public File getApiKeySummaryFile() {
        return StringUtils.isEmpty(apiKeySummaryFile) ? null : new File(apiKeySummaryFile);
    }

    public int getDayOfMonth() {
        return dayOfMonth;
    }

    /**
     * Build the Request body of the Elastic search request
     * @return request body
     * @throws IOException
     */
    public String getElasticSearchRequestBody() throws IOException {
        try (InputStream resourceAsStream = getClass().getResourceAsStream(getEsBodyFileName())) {
            List<String> lines = IOUtils.readLines(resourceAsStream, StandardCharsets.UTF_8);
            StringBuilder out = new StringBuilder();
            for (String line : lines) {
                out.append(line);
            }
            return out.toString();
        }
    }

    @Bean(name = ELASTIC_SEARCH_CONNECTION)
    public ElasticSearchConnection getElasticSearchConnection() throws IOException {
        return new ElasticSearchConnection(getElasticSearchUrl(), getElasticSearchRequestBody(), getObjectMapper());
    }

    @Bean(name = DATABOX)
    public Databox getDatabox() {
        return  new Databox(getDataboxToken());
    }


    @Bean(REGISTERED_CLIENT_CONNECTION)
    public ClientsServiceConnection getClientsServiceConnection() {
        return  new ClientsServiceConnection(
                getRegisteredClientsUrl(),
                AuthenticationBuilder.newAuthentication(
                        new AuthenticationConfig(loadProperties())),
                getObjectMapper());
    }

    private Properties loadProperties() {
        Properties properties = new Properties();
        properties.setProperty(AuthenticationConfig.CONFIG_TOKEN_ENDPOINT, tokenEndpoint);
        properties.setProperty(AuthenticationConfig.CONFIG_GRANT_PARAMS, grantParams);
        return properties;
    }

    @Primary
    @Bean
    public ObjectMapper getObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();

        SimpleModule module = new SimpleModule();
        mapper.registerModule(module);

        mapper.setVisibility(
                mapper.getVisibilityChecker()
                        .withCreatorVisibility(NONE)
                        .withFieldVisibility(NONE)
                        .withGetterVisibility(NONE)
                        .withIsGetterVisibility(NONE)
                        .withSetterVisibility(NONE));


        mapper.findAndRegisterModules();
        return  mapper;
    }
}
