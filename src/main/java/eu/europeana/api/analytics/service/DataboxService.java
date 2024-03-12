package eu.europeana.api.analytics.service;

import com.databox.sdk.Databox;
import eu.europeana.api.analytics.exception.DataboxPushFailedException;
import eu.europeana.api.analytics.utils.Constants;
import eu.europeana.api.analytics.utils.DataboxUtils;
import eu.europeana.api.commons.definitions.statistics.UsageStatsFields;
import eu.europeana.api.commons.definitions.statistics.entity.EntitiesPerLanguage;
import eu.europeana.api.commons.definitions.statistics.entity.EntityMetric;
import eu.europeana.api.commons.definitions.statistics.search.SearchMetric;
import eu.europeana.api.commons.definitions.statistics.set.SetMetric;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DataboxService implements StatsQuery {

    private static final Logger LOG = LogManager.getLogger(DataboxService.class);

    @Override
    public void execute(AnalyticsServiceClient analyticsServiceClient) throws DataboxPushFailedException {
        Databox databox = new Databox(analyticsServiceClient.getDataboxToken());
        pushMetrics(analyticsServiceClient, databox);

    }

    private void pushMetrics(AnalyticsServiceClient analyticsServiceClient, Databox databox) throws DataboxPushFailedException {
        long noOfusers = analyticsServiceClient.getUserStats();
        SetMetric galleryMetrics = analyticsServiceClient.getSetApiStats();
        EntityMetric entityMetrics = analyticsServiceClient.getEntityApiStats();
        SearchMetric searchMetric = analyticsServiceClient.getSearchApiStats();

        LOG.info("Databox push started using token {} ...... ", analyticsServiceClient.getDataboxToken());
        pushGalleryMetrics(galleryMetrics, databox);
        DataboxUtils.pushIndividualDataToDataBox(Constants.NUMBER_OF_USERS, noOfusers, databox);
        pushEntityMetrics(entityMetrics, databox);
        pushSearchApiMetrics(searchMetric, databox);
    }

    private void pushEntityMetrics(EntityMetric entityMetric, Databox databox) throws DataboxPushFailedException {
        if (entityMetric == null) {
            LOG.error("Error fetching entity statistics from entity api.");
            return;
        }
        // 1) push entity per type
        DataboxUtils.pushEntityPerTypeDataToDataBox(entityMetric.getEntitiesPerType(), databox);

        // 1) push entity per lang
        int count = 0;
        for (EntitiesPerLanguage entity : entityMetric.getEntitiesPerLanguages()) {
            DataboxUtils.pushEntityPerLanguageDataToDataBox(entity, databox);
            count ++;
        }
        // fallback check
        if (count != entityMetric.getEntitiesPerLanguages().size()) {
            throw new DataboxPushFailedException("All entities per lang/type are not pushed. Entities pushed "+count + "!= entities fetched "+ entityMetric.getEntitiesPerLanguages().size());
        }
    }

    private void pushGalleryMetrics(SetMetric galleryMetrics, Databox databox) throws DataboxPushFailedException {
        if (galleryMetrics == null) {
            LOG.error("Error fetching gallery statistics from set api.");
            return;
        }
        DataboxUtils.pushCollectiveGalleryDataToDataBox(galleryMetrics, databox);
    }

    private void pushSearchApiMetrics(SearchMetric searchMetric, Databox databox) throws DataboxPushFailedException {
        if (searchMetric == null) {
            LOG.error("Error fetching search statistics from search api.");
            return;
        }
        DataboxUtils.pushLinkedItems(searchMetric.getItemsLinkedToEntities(), databox);
        DataboxUtils.pushHighQualityMetric(searchMetric.getAllRecords(), databox, UsageStatsFields.ALL_RECORDS);
        DataboxUtils.pushHighQualityMetric(searchMetric.getAllCompliantRecords(), databox, UsageStatsFields.ALL_COMPLAINT_RECORDS);
        DataboxUtils.pushHighQualityMetric(searchMetric.getNonCompliantRecord(), databox, UsageStatsFields.NON_COMPLAINT_RECORDS);
        DataboxUtils.pushHighQualityMetric(searchMetric.getHighQualityData(), databox, UsageStatsFields.HIGH_QUALITY_DATA);
        DataboxUtils.pushHighQualityMetric(searchMetric.getHighQualityContent(), databox, UsageStatsFields.HIGH_QUALITY_CONTENT);
        DataboxUtils.pushHighQualityMetric(searchMetric.getHighQualityReusableContent(), databox, UsageStatsFields.HIGH_QUALITY_RESUABLE_CONTENT);
        DataboxUtils.pushHighQualityMetric(searchMetric.getHighQualityMetadata(), databox, UsageStatsFields.HIGH_QUALITY_METADATA);
    }

}
