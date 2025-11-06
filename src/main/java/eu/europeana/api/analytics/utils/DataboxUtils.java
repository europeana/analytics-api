package eu.europeana.api.analytics.utils;

import com.databox.sdk.Databox;
import com.databox.sdk.KPI;
import eu.europeana.api.analytics.exception.DataboxPushFailedException;
import eu.europeana.api.commons.definitions.statistics.UsageStatsFields;
import eu.europeana.api.commons.definitions.statistics.entity.EntitiesPerLanguage;
import eu.europeana.api.commons.definitions.statistics.entity.EntityStats;
import eu.europeana.api.commons.definitions.statistics.search.HighQualityMetric;
import eu.europeana.api.commons.definitions.statistics.set.SetMetric;
import eu.europeana.api.commons.definitions.statistics.user.ELKMetric;
import eu.europeana.api.commons.definitions.statistics.user.UserMetric;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

import static eu.europeana.api.commons.definitions.statistics.UsageStatsFields.*;

/**
 * Utils class for Databox Service
 * @author srishti singh
 */
public class DataboxUtils {

    private static final Logger LOG = LogManager.getLogger(DataboxUtils.class);

    /**
     * Method to push all the High quality data in one metric collectively.
     * With Attribute as the type of metric and Value as the value/count of the metric.
     * example :
     *  {"data": [ { "$keyValue": 145, "Type": "image"},
     *             { "$keyValue": 51, "Type": "text"},
     *             { "$keyValue": 296, "Type": "audio"},
     *             { "$keyValue": 6, "Type": "video"},
     *             { "$keyValue": 6, "Type": "3D"},
     *             { "$keyValues": 1, "Type": "total"}
     *         ]}
     *
     * @param linkedItemMetric
     * @param keyValue values can be allRecords, nonCompliantRecord, allCompliantRecords etc.
     * @param databox
     */
    public static void pushHighQualityMetric(HighQualityMetric linkedItemMetric, Databox databox, String keyValue) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(keyValue).setValue(linkedItemMetric.getImage()).addAttribute(Constants.TYPE_ATTRIBUTE, UsageStatsFields.IMAGE));
            kpis.add(new KPI().setKey(keyValue).setValue(linkedItemMetric.getText()).addAttribute(Constants.TYPE_ATTRIBUTE, UsageStatsFields.TEXT));
            kpis.add(new KPI().setKey(keyValue).setValue(linkedItemMetric.getAudio()).addAttribute(Constants.TYPE_ATTRIBUTE, UsageStatsFields.AUDIO));
            kpis.add(new KPI().setKey(keyValue).setValue(linkedItemMetric.getVideo()).addAttribute(Constants.TYPE_ATTRIBUTE, UsageStatsFields.VIDEO));
            kpis.add(new KPI().setKey(keyValue).setValue(linkedItemMetric.getThreeD()).addAttribute(Constants.TYPE_ATTRIBUTE, UsageStatsFields.THREE_D));
            kpis.add(new KPI().setKey(keyValue).setValue(linkedItemMetric.getAll()).addAttribute(Constants.TYPE_ATTRIBUTE, UsageStatsFields.ALL));

            databox.push(kpis);
            LOG.info("Successfully pushed the high quality metric data to databox");
        } catch (RuntimeException e) {
            throw new DataboxPushFailedException(keyValue, e.getLocalizedMessage());
        }
    }

    /**
     * Method to push all the linked tem data in one metric collectively.
     * With Attribute as the type of metric and Value as the value/count of the metric.
     * example :
     *  {"data": [ { "$itemsLinkedToEntities": 145, "Type": "agent"},
     *             { "$itemsLinkedToEntities": 51, "Type": "concept"},
     *             { "$itemsLinkedToEntities": 296, "Type": "organization"},
     *             { "$itemsLinkedToEntities": 6, "Type": "place"},
     *             { "$itemsLinkedToEntities": 6, "Type": "timespan"},
     *             { "$itemsLinkedToEntities": 1, "Type": "overall"}
     *         ]}
     *
     * @param linkedItemMetric
     * @param databox
     */
    public static void pushLinkedItems(EntityStats linkedItemMetric, Databox databox) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(Constants.ITEMS_LINKED_TO_ENTITIES).setValue(linkedItemMetric.getAgents()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.AGENT));
            kpis.add(new KPI().setKey(Constants.ITEMS_LINKED_TO_ENTITIES).setValue(linkedItemMetric.getConcepts()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.CONCEPT));
            kpis.add(new KPI().setKey(Constants.ITEMS_LINKED_TO_ENTITIES).setValue(linkedItemMetric.getOrganisations()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.ORGANISATION));
            kpis.add(new KPI().setKey(Constants.ITEMS_LINKED_TO_ENTITIES).setValue(linkedItemMetric.getTimespans()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.TIMESPAN));
            kpis.add(new KPI().setKey(Constants.ITEMS_LINKED_TO_ENTITIES).setValue(linkedItemMetric.getPlaces()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.PLACE));
            kpis.add(new KPI().setKey(Constants.ITEMS_LINKED_TO_ENTITIES).setValue(linkedItemMetric.getAll()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.ALL));

            databox.push(kpis);
            LOG.info("Successfully pushed the linked items for entites data to databox");
        } catch (RuntimeException e) {
            throw new DataboxPushFailedException(Constants.ITEMS_LINKED_TO_ENTITIES, e.getLocalizedMessage());
        }
    }

    /**
     * Method to push all the gallery data in one metric collectively.
     * With Attribute as the type of metric and Value as the value/count of the metric.
     * example :
     *  {"data": [ { "$GalleryMetrics": 145, "Type": "PublicSets"},
     *             { "$GalleryMetrics": 51, "Type": "PrivateSets"},
     *             { "$GalleryMetrics": 296, "Type": "ItemsLiked"},
     *             { "$GalleryMetrics": 6, "Type": "AverageSetsPerUser"},
     *             { "$GalleryMetrics": 6, "Type": "NumberOfUsersWithGallery"},
     *             { "$GalleryMetrics": 1, "Type": "NumberOfUsersWithLike"},
     *             { "$GalleryMetrics": 6, "Type": "NumberOfUsersWithLikeOrGallery"}
     *             { "$GalleryMetrics": 6, "Type": "NumberOfEntitySets"}
     *             { "$GalleryMetrics": 6, "Type": "NumberOfItemsInEntitySets"}
     *         ]}
     *
     * @param galleryMetricData
     * @param databox
     */
    public static void pushCollectiveGalleryDataToDataBox(SetMetric galleryMetricData, Databox databox) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getNoOfPublicSets()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.PUBLIC_SETS));
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getNoOfPrivateSets()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.PRIVATE_SETS));
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getNoOfItemsLiked()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.ITEMS_LIKED));
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getAverageSetsPerUser()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.SETS_PER_USER));
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getNumberOfUsersWithGallery()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.NUMBER_OF_USER_WITH_GALLERY));
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getNumberOfUsersWithLike()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.NUMBER_OF_USER_WITH_LIKE));
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getNumberOfUsersWithLikeOrGallery()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.NUMBER_OF_USER_WITH_LIKE_OR_GALLERY));
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getNumberOfEntitySets()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.NUMBER_OF_ENTITY_SETS));
            kpis.add(new KPI().setKey(Constants.GALLERY_METRICS).setValue(galleryMetricData.getNumberOfItemsInEntitySets()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.NUMBER_OF_ITEMS_IN_ENTITY_SETS));

            databox.push(kpis);
            LOG.info("Successfully pushed the gallery data to databox");
        } catch (RuntimeException e) {
            throw new DataboxPushFailedException(Constants.GALLERY_METRICS, e.getLocalizedMessage());
        }
    }

    /**
     * Method to push entity per lang data
     * With Attribute as the language and Value as the percentage values
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
    public static void pushEntityPerLanguageDataToDataBox(EntitiesPerLanguage entity, Databox databox) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(Constants.TIMESPAN).setValue(entity.getTimespans()).addAttribute(Constants.ENTITY_ATTRIBUTE_LANG, entity.getLang()));
            kpis.add(new KPI().setKey(Constants.CONCEPT).setValue(entity.getConcepts()).addAttribute(Constants.ENTITY_ATTRIBUTE_LANG, entity.getLang()));
            kpis.add(new KPI().setKey(Constants.ORGANISATION).setValue(entity.getOrganisations()).addAttribute(Constants.ENTITY_ATTRIBUTE_LANG, entity.getLang()));
            kpis.add(new KPI().setKey(Constants.AGENT).setValue(entity.getAgents()).addAttribute(Constants.ENTITY_ATTRIBUTE_LANG, entity.getLang()));
            kpis.add(new KPI().setKey(Constants.PLACE).setValue(entity.getPlaces()).addAttribute(Constants.ENTITY_ATTRIBUTE_LANG, entity.getLang()));
            kpis.add(new KPI().setKey(Constants.TOTAL).setValue(entity.getAll()).addAttribute(Constants.ENTITY_ATTRIBUTE_LANG, entity.getLang()));

            databox.push(kpis);
            LOG.info("Successfully pushed the entity data for language {} to databox", entity.getLang());
        } catch (RuntimeException e) {
            throw new DataboxPushFailedException("entity for " + Constants.ENTITY_ATTRIBUTE_LANG + " " + entity.getLang(), e.getLocalizedMessage());
        }
    }

    /**
     *  Method to push entity per type data
     * With Attribute as the Type of entity
     * ex : {"data":[ { "$EntityPerType": 11439.0, "Type": "Agent"},
     *                { "$EntityPerType": 4.0, "Type": "Concept"},
     *                { "$EntityPerType": 4.0, "Type": "Place"},
     *                { "$EntityPerType": 2.0, "Type": "Organization"},
     *                { "$EntityPerType": 0.0, "Type": "Timespan"},
     *                { "$EntityPerType": 11449.0, "Type": "Total"}]}
     *
     * @param entity
     * @param databox
     * @throws DataboxPushFailedException
     */
    public static void pushEntityPerTypeDataToDataBox(EntityStats entity, Databox databox) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(Constants.ENTITY_TYPE_METRICS).setValue(entity.getAgents()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.AGENT));
            kpis.add(new KPI().setKey(Constants.ENTITY_TYPE_METRICS).setValue(entity.getConcepts()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.CONCEPT));
            kpis.add(new KPI().setKey(Constants.ENTITY_TYPE_METRICS).setValue(entity.getPlaces()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.PLACE));
            kpis.add(new KPI().setKey(Constants.ENTITY_TYPE_METRICS).setValue(entity.getOrganisations()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.ORGANISATION));
            kpis.add(new KPI().setKey(Constants.ENTITY_TYPE_METRICS).setValue(entity.getTimespans()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.TIMESPAN));
            kpis.add(new KPI().setKey(Constants.ENTITY_TYPE_METRICS).setValue(entity.getAll()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.TOTAL));
            databox.push(kpis);
            LOG.info("Successfully pushed the entity per type data to databox");
        } catch (RuntimeException e) {
            throw new DataboxPushFailedException(Constants.ENTITY_TYPE_METRICS, e.getLocalizedMessage(), e);
        }
    }

    /**
     * Push user data
     *
     * @param userMetric user metric
     * @param databox databox instance
     * @throws DataboxPushFailedException exception during databox push
     */
    public static  void pushUserData(UserMetric userMetric, Databox databox) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(NumberOfUsers).setValue(userMetric.getNumberOfUsers()));

            if (userMetric.getRegisteredClients() != null) {
                kpis.add(new KPI().setKey(RegisteredClients).setValue(userMetric.getRegisteredClients().getPersonal())
                        .addAttribute(Constants.TYPE_ATTRIBUTE, UsageStatsFields.Personal));
                kpis.add(new KPI().setKey(RegisteredClients).setValue(userMetric.getRegisteredClients().getProject())
                        .addAttribute(Constants.TYPE_ATTRIBUTE, UsageStatsFields.Project));
                kpis.add(new KPI().setKey(RegisteredClients).setValue(userMetric.getRegisteredClients().getInternal())
                        .addAttribute(Constants.TYPE_ATTRIBUTE, UsageStatsFields.Internal));
            }
            databox.push(kpis);
            LOG.info("Successfully pushed the user and client data to databox");
        } catch (RuntimeException e) {
            throw new DataboxPushFailedException(NumberOfUsers + ", " + RegisteredClients, e.getLocalizedMessage(), e);
        }
    }

    /**
     * Push Elk metric
     * @param elkMetric elk metric
     * @param databox databox instance
     * @throws DataboxPushFailedException exception during databox push
     */
    public static  void pushElkData(ELKMetric elkMetric, Databox databox) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();

            kpis.add(new KPI().setKey(ClientUsage).setValue(elkMetric.getInternalClientUsage()).addAttribute(Constants.TYPE_ATTRIBUTE, Internal));
            kpis.add(new KPI().setKey(ClientUsage).setValue(elkMetric.getExternalClientUsage()).addAttribute(Constants.TYPE_ATTRIBUTE, External));

            kpis.add(new KPI().setKey(ActiveExternalClients).setValue(elkMetric.getRegularCustomer()).addAttribute(Constants.TYPE_ATTRIBUTE, RegularCustomer));
            kpis.add(new KPI().setKey(ActiveExternalClients).setValue(elkMetric.getAllCustomer()).addAttribute(Constants.TYPE_ATTRIBUTE, AllCustomer));

            kpis.add(new KPI().setKey(ActiveExternalClients).setValue(elkMetric.getRegularUser()).addAttribute(Constants.TYPE_ATTRIBUTE, RegularUser));
            kpis.add(new KPI().setKey(ActiveExternalClients).setValue(elkMetric.getAllUser()).addAttribute(Constants.TYPE_ATTRIBUTE, AllUser));

            kpis.add(new KPI().setKey(ActiveExternalClients).setValue(elkMetric.getAll()).addAttribute(Constants.TYPE_ATTRIBUTE, All));

            databox.push(kpis);
            LOG.info("Successfully pushed the user/account holders data to databox");
        } catch (RuntimeException e) {
            throw new DataboxPushFailedException(ClientUsage + ", " + ActiveExternalClients, e.getLocalizedMessage(), e);
        }
    }
}
