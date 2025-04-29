package eu.europeana.api.analytics.service;

import com.databox.sdk.Databox;
import eu.europeana.api.analytics.exception.DataboxPushFailedException;
import eu.europeana.api.analytics.utils.DataboxUtils;
import eu.europeana.api.commons.definitions.statistics.entity.EntitiesPerLanguage;
import eu.europeana.api.commons.definitions.statistics.entity.EntityMetric;
import eu.europeana.api.commons.definitions.statistics.search.SearchMetric;
import eu.europeana.api.commons.definitions.statistics.set.SetMetric;
import eu.europeana.api.commons.definitions.statistics.user.UserMetric;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static eu.europeana.api.commons.definitions.statistics.UsageStatsFields.*;


public class DataboxService implements StatsQuery {

    private static final Logger LOG = LogManager.getLogger(DataboxService.class);

    @Override
    public void execute(AnalyticsServiceClient analyticsServiceClient) throws DataboxPushFailedException {
        Databox databox = new Databox(analyticsServiceClient.getDataboxToken());
        pushMetrics(analyticsServiceClient, databox);
    }

    private void pushMetrics(AnalyticsServiceClient analyticsServiceClient, Databox databox) throws DataboxPushFailedException {
        UserMetric userMetric = analyticsServiceClient.getUserStats();
        SetMetric galleryMetrics = analyticsServiceClient.getSetApiStats();
        EntityMetric entityMetrics = analyticsServiceClient.getEntityApiStats();
        SearchMetric searchMetric = analyticsServiceClient.getSearchApiStats();

        LOG.info("Databox push started using token {} ...... ", analyticsServiceClient.getDataboxToken());
        pushGalleryMetrics(galleryMetrics, databox);
        pushUserMetrics(userMetric, databox);
        pushEntityMetrics(entityMetrics, databox);
        pushSearchApiMetrics(searchMetric, databox);
    }

    private void pushUserMetrics(UserMetric userMetric, Databox databox) throws DataboxPushFailedException {
        DataboxUtils.pushIndividualDataToDataBox(NumberOfUsers, userMetric.getNumberOfUsers(), databox);
        DataboxUtils.pushIndividualDataToDataBox(NumberOfProjectClients, userMetric.getNumberOfProjectClients(), databox);
        DataboxUtils.pushIndividualDataToDataBox(NumberOfPersonalClients, userMetric.getNumberOfPersonalClients(), databox);
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
        DataboxUtils.pushHighQualityMetric(searchMetric.getAllRecords(), databox, ALL_RECORDS);
        DataboxUtils.pushHighQualityMetric(searchMetric.getAllCompliantRecords(), databox, ALL_COMPLAINT_RECORDS);
        DataboxUtils.pushHighQualityMetric(searchMetric.getNonCompliantRecord(), databox, NON_COMPLAINT_RECORDS);
        DataboxUtils.pushHighQualityMetric(searchMetric.getHighQualityData(), databox, HIGH_QUALITY_DATA);
        DataboxUtils.pushHighQualityMetric(searchMetric.getHighQualityContent(), databox, HIGH_QUALITY_CONTENT);
        DataboxUtils.pushHighQualityMetric(searchMetric.getHighQualityReusableContent(), databox, HIGH_QUALITY_RESUABLE_CONTENT);
        DataboxUtils.pushHighQualityMetric(searchMetric.getHighQualityMetadata(), databox, HIGH_QUALITY_METADATA);
    }

}
