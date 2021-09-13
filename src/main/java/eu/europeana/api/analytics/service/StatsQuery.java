package eu.europeana.api.analytics.service;

import eu.europeana.api.analytics.exception.ClientResponseException;
import eu.europeana.api.analytics.exception.DataboxPushFailedException;

public interface StatsQuery {

    void execute(AnalyticsServiceClient analyticsServiceClient) throws DataboxPushFailedException, ClientResponseException;
}
