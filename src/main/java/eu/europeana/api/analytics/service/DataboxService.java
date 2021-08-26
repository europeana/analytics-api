package eu.europeana.api.analytics.service;

import com.databox.sdk.Databox;
import com.databox.sdk.KPI;
import eu.europeana.api.analytics.exception.DataboxPushFailedException;
import eu.europeana.api.commons.definitions.statistics.Metric;
import eu.europeana.api.analytics.utils.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class DataboxService implements StatsQuery {

    private static final Logger LOG = LogManager.getLogger(DataboxService.class);

    @Override
    public void execute(AnalyticsServiceClient analyticsServiceClient) throws DataboxPushFailedException {
        Databox databox = new Databox(analyticsServiceClient.getDataboxToken());
        pushMetrics(analyticsServiceClient, databox);

    }

    private void pushMetrics(AnalyticsServiceClient analyticsServiceClient, Databox databox) throws DataboxPushFailedException {
        long noOfusers = Long.parseLong(analyticsServiceClient.getUserStats());
        Metric galleryMetrics = analyticsServiceClient.getSetApiStats();
        LOG.info("Successfully fetched the gallery and user statistics");

        if(galleryMetrics != null) {
            LOG.info("Databox push started using token {} ", analyticsServiceClient.getDataboxToken());
            // collective gallery data
            pushCollectiveGalleryDataToDataBox(galleryMetrics, databox);
        } else {
            LOG.error("Error fetching gallery statistics from set api");
        }
        // push single metric data for user
        pushIndividualDataToDataBox(Constants.NUMBER_OF_USERS, noOfusers, databox);
    }

    /**
     * Method to push all the gallery data in one metric collectively.
     * With Attribute as the type of metric and Value as the value/count of the metric.
     *
     * For example : {"data" : [{"$GalleryData":<galleryMetricData.getNoOfPublicSets()>,"Type":"PublicSets"}]}
     *               {"data" : [{"$GalleryData":<galleryMetricData.getNoOfPrivateSets()>,"Type":"PrivateSets"}]}
     *
     * @param galleryMetricData
     * @param databox
     */
    private void pushCollectiveGalleryDataToDataBox(Metric galleryMetricData, Databox databox) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(Constants.COLLECTIVE_GALLERY_DATA).setValue(galleryMetricData.getNoOfPublicSets()).addAttribute(Constants.COLLECTIVE_GALLERY_ATTRIBUTE, Constants.PUBLIC_SETS));
            kpis.add(new KPI().setKey(Constants.COLLECTIVE_GALLERY_DATA).setValue(galleryMetricData.getNoOfPrivateSets()).addAttribute(Constants.COLLECTIVE_GALLERY_ATTRIBUTE, Constants.PRIVATE_SETS));
            kpis.add(new KPI().setKey(Constants.COLLECTIVE_GALLERY_DATA).setValue(galleryMetricData.getNoOfItemsLiked()).addAttribute(Constants.COLLECTIVE_GALLERY_ATTRIBUTE, Constants.ITEMS_LIKED));
            kpis.add(new KPI().setKey(Constants.COLLECTIVE_GALLERY_DATA).setValue(galleryMetricData.getAverageSetsPerUser()).addAttribute(Constants.COLLECTIVE_GALLERY_ATTRIBUTE, Constants.SETS_PER_USER));
            databox.push(kpis);
            LOG.info("Successfully pushed the gallery data to databox");
        } catch (RuntimeException e) {
            throw new DataboxPushFailedException(Constants.COLLECTIVE_GALLERY_DATA, e.getLocalizedMessage());
        }
    }

    /**
     * Method to push the data as single metric.
     *
     * This will be used mostly when we have single value data
     * that needs to be pushed into databox
     *
     * For example : {"data" : [{"$<type>":<value>}]}
     *
     * @param type
     * @param value
     * @param databox
     */
    private void pushIndividualDataToDataBox(String type, long value, Databox databox) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(type).setValue(value));
            databox.push(kpis);
            LOG.info("Successfully pushed the data to databox for {}", type);
        } catch (Exception e) {
            throw new DataboxPushFailedException(type, e.getLocalizedMessage());
        }
    }

}
