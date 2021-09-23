package eu.europeana.api.analytics.service;

import com.databox.sdk.Databox;
import com.databox.sdk.KPI;
import eu.europeana.api.analytics.exception.DataboxPushFailedException;
import eu.europeana.api.analytics.exception.ClientResponseException;
import eu.europeana.api.analytics.utils.Constants;
import eu.europeana.api.commons.definitions.statistics.entity.EntitiesPerLanguage;
import eu.europeana.api.commons.definitions.statistics.entity.EntityMetric;
import eu.europeana.api.commons.definitions.statistics.set.SetMetric;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class DataboxService implements StatsQuery {

    private static final Logger LOG = LogManager.getLogger(DataboxService.class);

    @Override
    public void execute(AnalyticsServiceClient analyticsServiceClient) throws DataboxPushFailedException, ClientResponseException {
        Databox databox = new Databox(analyticsServiceClient.getDataboxToken());
        pushMetrics(analyticsServiceClient, databox);

    }

    private void pushMetrics(AnalyticsServiceClient analyticsServiceClient, Databox databox) throws DataboxPushFailedException, ClientResponseException {
        long noOfusers = analyticsServiceClient.getUserStats();
        SetMetric galleryMetrics = analyticsServiceClient.getSetApiStats();
        EntityMetric entityMetrics = analyticsServiceClient.getEntityApiStats();

        LOG.info("Databox push started using token {} ", analyticsServiceClient.getDataboxToken());
        pushGalleryMetrics(galleryMetrics, databox);
        pushIndividualDataToDataBox(Constants.NUMBER_OF_USERS, noOfusers, databox);
        pushEntityMetrics(entityMetrics, databox);
    }

    private void pushEntityMetrics(EntityMetric entityMetric, Databox databox) throws ClientResponseException, DataboxPushFailedException {
        if (entityMetric == null) {
            throw new ClientResponseException("Error fetching entity statistics from entity api.");
        }
        LOG.info("Successfully fetched the entity statistics");
        int count = 0;
        for (EntitiesPerLanguage entity : entityMetric.getEntities()) {
            pushCollectiveEntityDataToDataBox(entity, databox);
            count ++;
        }
        // fallback check
        if (count != entityMetric.getEntities().size()) {
            throw new DataboxPushFailedException("All entities per lang/type are not pushed. Entities pushed "+count + "!= entities fetched "+ entityMetric.getEntities().size());
        }

    }

    /**
     * Method to push all the gallery data in one metric collectively.
     * With Attribute as the type of metric and Value as the value/count of the metric.
     * example :
     *  {"data" : [{"$Timespan":100,"language":"en"},
     *             {"$Concept":94,"language":"en"},
     *             {"$Organisation":27,"language":"en"},
     *             {"$Agent":100,"language":"en"},
     *             {"$Place":6,"language":"en"},
     *             {"$Total":47,"language":"en"}
     *   ]}
     *
     * @param entity
     * @param databox
     */
    private void pushCollectiveEntityDataToDataBox(EntitiesPerLanguage entity, Databox databox) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(Constants.TIMESPAN).setValue(entity.getTimespans()).addAttribute(Constants.ENTITY_ATTRIBUTE, entity.getLang()));
            kpis.add(new KPI().setKey(Constants.CONCEPT).setValue(entity.getConcepts()).addAttribute(Constants.ENTITY_ATTRIBUTE, entity.getLang()));
            kpis.add(new KPI().setKey(Constants.ORGANISATION).setValue(entity.getOrganisations()).addAttribute(Constants.ENTITY_ATTRIBUTE, entity.getLang()));
            kpis.add(new KPI().setKey(Constants.AGENT).setValue(entity.getAgents()).addAttribute(Constants.ENTITY_ATTRIBUTE, entity.getLang()));
            kpis.add(new KPI().setKey(Constants.PLACE).setValue(entity.getPlaces()).addAttribute(Constants.ENTITY_ATTRIBUTE, entity.getLang()));
            kpis.add(new KPI().setKey(Constants.TOTAL).setValue(entity.getTotal()).addAttribute(Constants.ENTITY_ATTRIBUTE, entity.getLang()));

            databox.push(kpis);
            LOG.info("Successfully pushed the entity data for language {} to databox", entity.getLang());
        } catch (RuntimeException e) {
            throw new DataboxPushFailedException("entity for " + Constants.ENTITY_ATTRIBUTE + " " + entity.getLang(), e.getLocalizedMessage());
        }
    }

    private void pushGalleryMetrics(SetMetric galleryMetrics, Databox databox) throws ClientResponseException, DataboxPushFailedException {
        if (galleryMetrics == null) {
            throw new ClientResponseException("Error fetching gallery statistics from set api.");
        }
        LOG.info("Successfully fetched the gallery statistics");
        pushCollectiveGalleryDataToDataBox(galleryMetrics, databox);
    }

    /**
     * Method to push all the gallery data in one metric collectively.
     * With Attribute as the type of metric and Value as the value/count of the metric.
     * example :
     *  {"data": [ { "$GalleryMetrics": 145, "Type": "PublicSets"},
     *             { "$GalleryMetrics": 51, "Type": "PrivateSets"},
     *             { "$GalleryMetrics": 296, "Type": "ItemsLiked"},
     *             { "$GalleryMetrics": 6, "Type": "AverageSetsPerUser"}
     *         ]}
     *
     * @param galleryMetricData
     * @param databox
     */
    private void pushCollectiveGalleryDataToDataBox(SetMetric galleryMetricData, Databox databox) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getNoOfPublicSets()).addAttribute(Constants.GALLERY_ATTRIBUTE, Constants.PUBLIC_SETS));
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getNoOfPrivateSets()).addAttribute(Constants.GALLERY_ATTRIBUTE, Constants.PRIVATE_SETS));
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getNoOfItemsLiked()).addAttribute(Constants.GALLERY_ATTRIBUTE, Constants.ITEMS_LIKED));
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getAverageSetsPerUser()).addAttribute(Constants.GALLERY_ATTRIBUTE, Constants.SETS_PER_USER));
            databox.push(kpis);
            LOG.info("Successfully pushed the gallery data to databox");
        } catch (RuntimeException e) {
            throw new DataboxPushFailedException(Constants.GALLERY_METRICS, e.getLocalizedMessage());
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
