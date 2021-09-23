package eu.europeana.api.analytics.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.api.analytics.exception.DataboxPushFailedException;
import eu.europeana.api.analytics.exception.ClientResponseException;
import eu.europeana.api.commons.definitions.statistics.Metric;
import eu.europeana.api.analytics.utils.Constants;
import eu.europeana.api.commons.definitions.statistics.UsageStatsFields;
import eu.europeana.api.commons.definitions.statistics.entity.EntityMetric;
import eu.europeana.api.commons.definitions.statistics.set.SetMetric;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.io.IOException;

@Service
public class AnalyticsServiceClient {

    private static final Logger LOG = LogManager.getLogger(AnalyticsServiceClient.class);

    @Value("${set.api.stats.url}")
    private String setApiStatsUrl;

    @Value("${apikey.stats.url}")
    private String userStatsUrl;

    @Value("${entity.stats.url}")
    private String entityStatsUrl;

    @Value("${databox.token}")
    private String databoxToken;

    private RestTemplate restTemplate = new RestTemplate();
    private ObjectMapper mapper;

    @PostConstruct
    public void init() {
        mapper = new ObjectMapper();

    }

    public String getSetApiStatsUrl() {
        return setApiStatsUrl;
    }

    public String getUserStatsUrl() {
        return userStatsUrl;
    }

    public String getEntityStatsUrl() {
        return entityStatsUrl;
    }

    public String getDataboxToken() {
        return databoxToken;
    }

    public void execute() throws DataboxPushFailedException, ClientResponseException {
        StatsQuery statsQuery = new DataboxService();
        statsQuery.execute(this);
    }

    /**
     * Method to fetch the statistics from apikey.
     * @return
     */
    public long getUserStats() throws ClientResponseException {
        LOG.info("Fetching the user statistics from url {} ", this::getUserStatsUrl);
        String json = restTemplate.getForObject(getUserStatsUrl(), String.class);
        if(json == null || json.isEmpty()) {
            throw new ClientResponseException(this.getUserStatsUrl(), "");
        }
        if (!json.contains(Constants.NUMBER_OF_USERS)) {
            throw new ClientResponseException(Constants.NUMBER_OF_USERS + " field not present in user stats response");
        }
        JSONObject jsonObject = new JSONObject(json);
        return Long.parseLong(jsonObject.getString(Constants.NUMBER_OF_USERS));
    }

    /**
     * Method to fetch the statistics from set-api
     * @return
     */
    public SetMetric getSetApiStats() {
        LOG.info("Fetching the gallery statistics from url {} ", this::getSetApiStatsUrl);
        try {
            String json = restTemplate.getForObject(getSetApiStatsUrl(), String.class);
            if (json != null && !json.isEmpty() && json.contains(UsageStatsFields.TYPE)) {
                return mapper.readValue(json, SetMetric.class);
            }
        } catch (IOException e) {
            LOG.error("Exception when deserializing response.", e);
        }
        return null;
    }

    /**
     * Method to fetch the statistics from entity-api v2
     * @return
     */
    public EntityMetric getEntityApiStats() {
        LOG.info("Fetching the entity statistics from url {} ", this::getEntityStatsUrl);
        try {
            String json = restTemplate.getForObject(getEntityStatsUrl(), String.class);
            if (json != null && !json.isEmpty() && json.contains(UsageStatsFields.TYPE)) {
                return mapper.readValue(json, EntityMetric.class);
            }
        } catch (IOException e) {
            LOG.error("Exception when deserializing response.", e);
        }
        return null;
    }

}
