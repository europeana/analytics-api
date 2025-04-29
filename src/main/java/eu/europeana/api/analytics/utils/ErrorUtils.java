package eu.europeana.api.analytics.utils;

import eu.europeana.api.commons.definitions.statistics.UsageStatsFields;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class ErrorUtils {

    private static final Logger LOG = LogManager.getLogger(ErrorUtils.class);

    public static final String Error_fetching_response = "Error fetching response from {} ";
    public static final String Exception_when_deserializing = " Exception when deserializing response.";


    public static void logErrors(String json, String url, List<String> fields) {
        if (json == null || json.isEmpty()) {
            LOG.error(Error_fetching_response, url);
        }
        if (json != null) {
            fields.stream().forEach(field -> {
                if (!json.contains(field)) {
                    LOG.error("{} field not present in user stats response", field);
                }
            });
        }
    }

    public static void logErrors(String json, String url) {
        if (json == null || json.isEmpty()) {
            LOG.error(Error_fetching_response, url);
        }
        if (json != null) {
            if (!json.contains(UsageStatsFields.ITEMS_LINKED_TO_ENTITIES)) {
                LOG.error(" {} field not present in search api stats response", UsageStatsFields.ITEMS_LINKED_TO_ENTITIES);
            }
            if (!json.contains(UsageStatsFields.ALL_RECORDS) || !json.contains(UsageStatsFields.NON_COMPLAINT_RECORDS) ||
                    !json.contains(UsageStatsFields.ALL_COMPLAINT_RECORDS) || !json.contains(UsageStatsFields.HIGH_QUALITY_DATA) ||
                    !json.contains(UsageStatsFields.HIGH_QUALITY_CONTENT) || !json.contains(UsageStatsFields.HIGH_QUALITY_RESUABLE_CONTENT) ||
                    !json.contains(UsageStatsFields.HIGH_QUALITY_METADATA)) {
                LOG.error("High quality metric data not present in search api stats response");
            }
        }
    }
}
