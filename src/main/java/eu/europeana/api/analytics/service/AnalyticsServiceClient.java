package eu.europeana.api.analytics.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.api.analytics.exception.DataboxPushFailedException;
import eu.europeana.api.analytics.utils.Constants;
import eu.europeana.api.commons.definitions.statistics.UsageStatsFields;
import eu.europeana.api.commons.definitions.statistics.entity.EntityMetric;
import eu.europeana.api.commons.definitions.statistics.search.SearchMetric;
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

    @Value("${search.api.stats.url}")
    private String searchApiUrl;

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

    public String getSearchApiUrl() {
        return searchApiUrl;
    }

    public String getDataboxToken() {
        return databoxToken;
    }

    public void execute() throws DataboxPushFailedException {
        StatsQuery statsQuery = new DataboxService();
        statsQuery.execute(this);
    }

    /**
     * Method to fetch the statistics from apikey.
     * @return
     */
    public long getUserStats() {
        LOG.info("Fetching the user statistics from url {}", this::getUserStatsUrl);
        String json = restTemplate.getForObject(getUserStatsUrl(), String.class);
        if(json == null || json.isEmpty()) {
            LOG.error("Error fetching response from {} ", this.getUserStatsUrl());
        }
        if (!json.contains(Constants.NUMBER_OF_USERS)) {
            LOG.error("{} field not present in user stats response",Constants.NUMBER_OF_USERS);
        }
        JSONObject jsonObject = new JSONObject(json);
        return Long.parseLong(jsonObject.getString(Constants.NUMBER_OF_USERS));
    }

    /**
     * Method to fetch the statistics from set-api
     * @return
     */
    public SetMetric getSetApiStats() {
        LOG.info("Fetching the gallery statistics from url {}", this::getSetApiStatsUrl);
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
        LOG.info("Fetching the entity statistics from url {}", this::getEntityStatsUrl);
        try {
            String json = restTemplate.getForObject(getEntityStatsUrl(), String.class);
            if (json == null || json.isEmpty()) {
                LOG.error("Error fetching response from {} ", this.getEntityStatsUrl());
            }
            if (!json.contains(UsageStatsFields.ENTITIES_PER_LANG)) {
                LOG.error("{} field not present in entity stats response", UsageStatsFields.ENTITIES_PER_LANG);
            }
           return mapper.readValue(json, EntityMetric.class);

        } catch (IOException e) {
            LOG.error("Exception when deserializing response.", e);
        }
        return null;
    }

    /**
     * Method to fetch the statistics from search api
     * @return
     */
    public SearchMetric getSearchApiStats() {
        LOG.info("Fetching the search statistics from url {}", this::getSearchApiUrl);
        try {
            String json = restTemplate.getForObject(getSearchApiUrl(), String.class);
            if (json == null || json.isEmpty()) {
                LOG.error("Error fetching response from {} ", this.getSearchApiUrl());
            }
            if (!json.contains(UsageStatsFields.ITEMS_LINKED_TO_ENTITIES)) {
                LOG.error(" {} field not present in search api stats response", UsageStatsFields.ITEMS_LINKED_TO_ENTITIES);
            }
            if(!json.contains(UsageStatsFields.ALL_RECORDS) || !json.contains(UsageStatsFields.NON_COMPLAINT_RECORDS) ||
                    !json.contains(UsageStatsFields.ALL_COMPLAINT_RECORDS) || !json.contains(UsageStatsFields.HIGH_QUALITY_DATA) ||
                    !json.contains(UsageStatsFields.HIGH_QUALITY_CONTENT) || !json.contains(UsageStatsFields.HIGH_QUALITY_RESUABLE_CONTENT) ||
                    !json.contains(UsageStatsFields.HIGH_QUALITY_METADATA)) {
                LOG.error("High quality metric data not present in search api stats response");
            }
            return mapper.readValue(json, SearchMetric.class);

        } catch (IOException e) {
            LOG.error("Exception when deserializing response.", e);
        }
        return null;
    }

}
