package eu.europeana.api.analytics.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.api.analytics.config.AnalyticsApiConfig;
import eu.europeana.api.analytics.exception.ApiKeyStatisticsException;
import eu.europeana.api.analytics.exception.DataboxPushFailedException;
import eu.europeana.api.commons.auth.AuthenticationHandler;
import eu.europeana.api.commons.definitions.statistics.entity.EntityMetric;
import eu.europeana.api.commons.definitions.statistics.search.SearchMetric;
import eu.europeana.api.commons.definitions.statistics.set.SetMetric;
import eu.europeana.api.commons.definitions.statistics.user.ELKMetric;
import eu.europeana.api.commons.definitions.statistics.user.UserMetric;
import eu.europeana.api.commons.http.HttpConnection;
import eu.europeana.api.commons.http.HttpResponseHandler;
import jakarta.annotation.Resource;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;

import static eu.europeana.api.analytics.utils.Constants.ANALYTICS_API_AUTH;

/**
 * Analytics Api service class.
 * @author srishti singh
 */
@Service
public class AnalyticsService {

    private static final Logger LOG = LogManager.getLogger(AnalyticsService.class);

    @Resource
    private AnalyticsApiConfig analyticsApiConfig;

    @Resource(name = ANALYTICS_API_AUTH)
    private AuthenticationHandler authHandler;

    private final ApiKeyStatsService apiKeyStatsService;
    private final DataboxService databoxService;

    private HttpConnection httpConnection = new HttpConnection(true);
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
        LOG.error("Successfully pushed the monthly task data to databox...... ");
    }

    /**
     * Method to fetch the statistics from apikey.
     * @return
     */
    private UserMetric getUserStats() {
        try {
            LOG.info("Fetching the user statistics from url {}", analyticsApiConfig.getUserStatsUrl());
            HttpResponseHandler response = httpConnection.get(analyticsApiConfig.getUserStatsUrl(), "application/json", authHandler);
            if (response.getStatus() == HttpStatus.SC_OK) {
                return mapper.readValue(response.getResponse(), UserMetric.class);
            }
        } catch (IOException e) {
            LOG.error("Error fetching response from {}", analyticsApiConfig.getUserStatsUrl(), e);
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
            HttpResponseHandler response = httpConnection.get(analyticsApiConfig.getSetApiStatsUrl(), "application/json", authHandler);
            if (response.getStatus() == HttpStatus.SC_OK) {
                return mapper.readValue(response.getResponse(), SetMetric.class);
            }
        } catch (IOException e) {
            LOG.error("Error fetching response from {}", analyticsApiConfig.getSetApiStatsUrl(), e);
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
            HttpResponseHandler response = httpConnection.get(analyticsApiConfig.getEntityStatsUrl(), "application/json", authHandler);
            if (response.getStatus() == HttpStatus.SC_OK) {
                return mapper.readValue(response.getResponse(), EntityMetric.class);}
        } catch (IOException e) {
            LOG.error("Error fetching response from {}", analyticsApiConfig.getEntityStatsUrl(), e);
        }
        return null;
    }

    /**
     * Method to fetch the statistics from search api
     * @return
     */
    private SearchMetric getSearchApiStats() throws DataboxPushFailedException {
        LOG.info("Fetching the search statistics from url {}", analyticsApiConfig.getSearchApiUrl());
        try {
            HttpResponseHandler response = httpConnection.get(analyticsApiConfig.getSearchApiUrl(), "application/json", authHandler);
            if (response.getStatus() == HttpStatus.SC_OK) {
                return mapper.readValue(response.getResponse(), SearchMetric.class);
            }
            // TODO : Temp fix , should be removed once we have implemented EA-4346
            if (response.getStatus() == HttpStatus.SC_GATEWAY_TIMEOUT) {
                throw new DataboxPushFailedException("Gateway Timeout from SR API !! ");
            }
        } catch (IOException e) {
            LOG.error("Error fetching response from {}", analyticsApiConfig.getSearchApiUrl(), e);
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
