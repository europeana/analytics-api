package eu.europeana.api.analytics.service;

import com.databox.sdk.Databox;
import eu.europeana.api.analytics.exception.DataboxPushFailedException;
import eu.europeana.api.analytics.utils.DataboxUtils;
import eu.europeana.api.commons.definitions.statistics.entity.EntitiesPerLanguage;
import eu.europeana.api.commons.definitions.statistics.entity.EntityMetric;
import eu.europeana.api.commons.definitions.statistics.search.SearchMetric;
import eu.europeana.api.commons.definitions.statistics.set.SetMetric;
import eu.europeana.api.commons.definitions.statistics.user.ELKMetric;
import eu.europeana.api.commons.definitions.statistics.user.UserMetric;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import static eu.europeana.api.commons.definitions.statistics.UsageStatsFields.*;

/**
 * databox service class
 * @author srishti singh
 */
@Service
public class DataboxService {

    private static final Logger LOG = LogManager.getLogger(DataboxService.class);

    protected final Databox databox;

    /**
     * Constructor
     * @param databox datobox token
     */
    @Autowired
    public DataboxService(Databox databox) {
        this.databox = databox;
    }

    protected void pushElkMetrics(ELKMetric elkMetric) throws DataboxPushFailedException {
        if (elkMetric == null) {
            LOG.error("Error fetching elk statistics.");
            return;
        }
        DataboxUtils.pushElkData(elkMetric, databox);
    }

    protected void pushUserMetrics(UserMetric userMetric) throws DataboxPushFailedException {
       if (userMetric == null) {
           LOG.error("Error fetching user statistics from auth.");
           return;
       }
       DataboxUtils.pushUserData(userMetric, databox);
    }

    protected void pushEntityMetrics(EntityMetric entityMetric) throws DataboxPushFailedException {
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

    protected void pushGalleryMetrics(SetMetric galleryMetrics) throws DataboxPushFailedException {
        if (galleryMetrics == null) {
            LOG.error("Error fetching gallery statistics from set api.");
            return;
        }
        DataboxUtils.pushCollectiveGalleryDataToDataBox(galleryMetrics, databox);
    }

    protected void pushSearchApiMetrics(SearchMetric searchMetric) throws DataboxPushFailedException {
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
