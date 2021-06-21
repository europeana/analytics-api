package eu.europeana.api.analytics.service;

import eu.europeana.api.analytics.model.Metric;

public class DataboxService implements StatsQuery {


    @Override
    public void execute(AnalyticsServiceClient analyticsServiceClient) {
        pushMetrics(analyticsServiceClient);

    }

    private void pushMetrics(AnalyticsServiceClient analyticsServiceClient) {

       long noOfusers = Long.parseLong(analyticsServiceClient.getUserStats());
        Metric gallerymetrics = analyticsServiceClient.getSetApiStats();
        System.out.println(noOfusers);
        System.out.println(gallerymetrics);

    }

    //JSON_STRING='{"data" : [{"$GalleryMetrics":'$count',"TypeOfMetrics":"'$typeOfMetrics'"}]}'
    private void galleryMetrics(Metric galleryMetricData) {
        StringBuilder jsonBody = new StringBuilder("{\"data\":[{\"");
        jsonBody.append("$GalleryData");
        jsonBody.append("\":\'");
        jsonBody.append("");

    }
}
