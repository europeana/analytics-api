package eu.europeana.api.analytics.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.api.analytics.model.Metric;
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

    private RestTemplate restTemplate = new RestTemplate();
    private ObjectMapper mapper;

    private StatsQuery statsQuery;

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

    public void execute() {
        statsQuery = new DataboxService();
        statsQuery.execute(this);
    }

    public String getUserStats() {
        String json = restTemplate.getForObject(getUserStatsUrl(), String.class);
        JSONObject jsonObject = new JSONObject(json);
        return jsonObject.getString("NumberOfUsers");
    }

    public Metric getSetApiStats() {
        String json = restTemplate.getForObject(getSetApiStatsUrl(), String.class);
        try {
            return mapper.readValue(json, Metric.class);
        } catch (IOException e) {
            LOG.error("Exception when deserializing response.", e);
        }
        return null;
    }

}
