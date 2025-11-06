package eu.europeana.api.analytics.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.api.analytics.config.AnalyticsApiConfig;
import eu.europeana.api.analytics.exception.ApiKeyStatisticsException;
import eu.europeana.api.analytics.exception.DataboxPushFailedException;
import eu.europeana.api.commons.definitions.statistics.UsageStatsFields;
import eu.europeana.api.commons.definitions.statistics.entity.EntityMetric;
import eu.europeana.api.commons.definitions.statistics.search.SearchMetric;
import eu.europeana.api.commons.definitions.statistics.set.SetMetric;
import eu.europeana.api.commons.definitions.statistics.user.ELKMetric;
import eu.europeana.api.commons.definitions.statistics.user.UserMetric;
import jakarta.annotation.Resource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.util.Arrays;

import static eu.europeana.api.analytics.utils.ErrorUtils.*;
import static eu.europeana.api.commons.definitions.statistics.UsageStatsFields.*;

/**
 * Analytics Api service class.
 * @author srishti singh
 */
@Service
public class AnalyticsService {

    private static final Logger LOG = LogManager.getLogger(AnalyticsService.class);

    @Resource
    private AnalyticsApiConfig analyticsApiConfig;

    private final ApiKeyStatsService apiKeyStatsService;
    private final DataboxService databoxService;

    private RestTemplate restTemplate = new RestTemplate();
    private ObjectMapper mapper = new ObjectMapper();

    /**
     * Constructor
     * @param apiKeyStatsService apikey stats service
     * @param databoxService databox service
     */
    @Autowired
    public AnalyticsService(ApiKeyStatsService apiKeyStatsService, DataboxService databoxService) {
        this.apiKeyStatsService = apiKeyStatsService;
        this.databoxService = databoxService;
    }

    /**
     * daily tasks to be exceuted
     * @throws DataboxPushFailedException if databox fails to push
     */
    public void executeDailyTask() throws DataboxPushFailedException {
        UserMetric   userMetric      = getUserStats();
        SetMetric    galleryMetrics  = getSetApiStats();
        EntityMetric entityMetrics   = getEntityApiStats();
        SearchMetric searchMetric    = getSearchApiStats();

        databoxService.pushUserMetrics(userMetric);
        databoxService.pushGalleryMetrics(galleryMetrics);
        databoxService.pushEntityMetrics(entityMetrics);
        databoxService.pushSearchApiMetrics(searchMetric);

    }

    /**
     * Monthly tasks to be exceuted
     * @throws DataboxPushFailedException if databox fails to push
     */
    public void executeMonthlyTask() throws DataboxPushFailedException {
        ELKMetric    elkMetric       = getELKStats();
        databoxService.pushElkMetrics(elkMetric);
    }

    /**
     * Method to fetch the statistics from apikey.
     * @return
     */
    private UserMetric getUserStats() {
        try {
            LOG.info("Fetching the user statistics from url {}", analyticsApiConfig.getUserStatsUrl());
            String json = restTemplate.getForObject(analyticsApiConfig.getUserStatsUrl(), String.class);
            logErrors(json,
                    analyticsApiConfig.getUserStatsUrl(),
                    Arrays.asList(NumberOfUsers, RegisteredClients));
            return mapper.readValue(json, UserMetric.class);
        } catch (IOException e) {
            LOG.error(Error_fetching_response, analyticsApiConfig.getUserStatsUrl(), e);
        }
        return null;
    }

    /**
     * Method to fetch the statistics from set-api
     * @return
     */
    private SetMetric getSetApiStats() {
        LOG.info("Fetching the gallery statistics from url {}", analyticsApiConfig.getSetApiStatsUrl());
        try {
            String json = restTemplate.getForObject(analyticsApiConfig.getSetApiStatsUrl(), String.class);
            if (json != null && !json.isEmpty() && json.contains(UsageStatsFields.TYPE)) {
                return mapper.readValue(json, SetMetric.class);
            }
        } catch (IOException e) {
            LOG.error(Exception_when_deserializing, e);
        }
        return null;
    }

    /**
     * Method to fetch the statistics from entity-api v2
     * @return
     */
    private EntityMetric getEntityApiStats() {
        LOG.info("Fetching the entity statistics from url {}", analyticsApiConfig.getEntityStatsUrl());
        try {
            String json = restTemplate.getForObject(analyticsApiConfig.getEntityStatsUrl(), String.class);
            if (json == null || json.isEmpty()) {
                LOG.error(Error_fetching_response, analyticsApiConfig.getEntityStatsUrl());
            }
            if (json != null && !json.contains(UsageStatsFields.ENTITIES_PER_LANG)) {
                LOG.error("{} field not present in entity stats response", UsageStatsFields.ENTITIES_PER_LANG);
            }
           return mapper.readValue(json, EntityMetric.class);

        } catch (IOException e) {
            LOG.error(Exception_when_deserializing, e);
        }
        return null;
    }

    /**
     * Method to fetch the statistics from search api
     * @return
     */
    private SearchMetric getSearchApiStats() {
        LOG.info("Fetching the search statistics from url {}", analyticsApiConfig.getSearchApiUrl());
        try {
            String json = restTemplate.getForObject(analyticsApiConfig.getSearchApiUrl(), String.class);
            logErrors(json, analyticsApiConfig.getSearchApiUrl());
            return mapper.readValue(json, SearchMetric.class);
        } catch (IOException e) {
            LOG.error(Exception_when_deserializing, e);
        }
        return null;
    }

    private ELKMetric getELKStats() {
        LOG.info("Fetching the apikey statistics from keycloak and elk - {}", analyticsApiConfig.getElasticSearchUrl());
        try {
            return apiKeyStatsService.generate();
        } catch (ApiKeyStatisticsException e) {
            LOG.error("Error while generating Elk stats {} ", e.getMessage(), e);
        }
        return null;
    }
}
