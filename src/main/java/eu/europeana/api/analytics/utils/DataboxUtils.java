package eu.europeana.api.analytics.utils;

import com.databox.sdk.Databox;
import com.databox.sdk.KPI;
import eu.europeana.api.analytics.exception.DataboxPushFailedException;
import eu.europeana.api.commons.definitions.statistics.UsageStatsFields;
import eu.europeana.api.commons.definitions.statistics.entity.EntitiesPerLanguage;
import eu.europeana.api.commons.definitions.statistics.entity.EntityStats;
import eu.europeana.api.commons.definitions.statistics.search.HighQualityMetric;
import eu.europeana.api.commons.definitions.statistics.set.SetMetric;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class DataboxUtils {

    private static final Logger LOG = LogManager.getLogger(DataboxUtils.class);

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
    public static void pushIndividualDataToDataBox(String type, long value, Databox databox) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(type).setValue(value));
            databox.push(kpis);
            LOG.info("Successfully pushed the data to databox for {}", type);
        } catch (Exception e) {
            throw new DataboxPushFailedException(type, e.getLocalizedMessage());
        }
    }

    /**
     * Method to push all the High quality data in one metric collectively.
     * With Attribute as the type of metric and Value as the value/count of the metric.
     * example :
     *  {"data": [ { "$keyValue": 145, "Type": "image"},
     *             { "$keyValue": 51, "Type": "text"},
     *             { "$keyValue": 296, "Type": "audio"},
     *             { "$keyValue": 6, "Type": "video"},
     *             { "$keyValue": 6, "Type": "3D"},
     *             { "$keyValues": 1, "Type": "all"}
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
     *             {"$All":47,"language":"en"}
     *   ]}
     *
     * @param entity
     * @param databox
     */
    public static void pushEntityPerLanguageDataToDataBox(EntitiesPerLanguage entity, Databox databox, String appendKey) throws DataboxPushFailedException {
        try {
            String append = appendKey + Constants.SEPERATOR;
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(append + Constants.TIMESPAN).setValue(entity.getTimespans()).addAttribute(Constants.ENTITY_ATTRIBUTE_LANG, entity.getLang()));
            kpis.add(new KPI().setKey(append + Constants.CONCEPT).setValue(entity.getConcepts()).addAttribute(Constants.ENTITY_ATTRIBUTE_LANG, entity.getLang()));
            kpis.add(new KPI().setKey(append + Constants.ORGANISATION).setValue(entity.getOrganisations()).addAttribute(Constants.ENTITY_ATTRIBUTE_LANG, entity.getLang()));
            kpis.add(new KPI().setKey(append + Constants.AGENT).setValue(entity.getAgents()).addAttribute(Constants.ENTITY_ATTRIBUTE_LANG, entity.getLang()));
            kpis.add(new KPI().setKey(append + Constants.PLACE).setValue(entity.getPlaces()).addAttribute(Constants.ENTITY_ATTRIBUTE_LANG, entity.getLang()));
            kpis.add(new KPI().setKey(appendKey).setValue(entity.getAll()).addAttribute(Constants.ENTITY_ATTRIBUTE_LANG, entity.getLang()));

            databox.push(kpis);
            LOG.info("Successfully pushed the entity data for language {} to databox", entity.getLang());
        } catch (RuntimeException e) {
            throw new DataboxPushFailedException("entity for " + appendKey + " " + entity.getLang(), e.getLocalizedMessage());
        }
    }

    /**
     *  Method to push entity per type data
     * With Attribute as the Type of entity
     * ex : {"data":[ { "$Key": 11439.0, "Type": "Agent"},
     *                { "$Key": 4.0, "Type": "Concept"},
     *                { "$key": 4.0, "Type": "Place"},
     *                { "$key": 2.0, "Type": "Organization"},
     *                { "$Key": 0.0, "Type": "Timespan"},
     *                { "$Key": 11449.0, "Type": "All"}]}
     *
     * @param entity
     * @param databox
     * @throws DataboxPushFailedException
     */
    public static void pushEntityDataToDataBox(EntityStats entity, Databox databox, String key) throws DataboxPushFailedException {
        try {
            List<KPI> kpis = new ArrayList<>();
            kpis.add(new KPI().setKey(key).setValue(entity.getAgents()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.PERSON));
            kpis.add(new KPI().setKey(key).setValue(entity.getConcepts()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.TOPIC));
            kpis.add(new KPI().setKey(key).setValue(entity.getPlaces()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.PLACE));
            kpis.add(new KPI().setKey(key).setValue(entity.getOrganisations()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.ORGANISATION));
            kpis.add(new KPI().setKey(key).setValue(entity.getTimespans()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.TIMESPAN));
            // for ItemsLinkedToEntities key the attribute is 'any'
            if (StringUtils.equals(key, Constants.ITEMS_LINKED_TO_ENTITIES_KEY)) {
                kpis.add(new KPI().setKey(key).setValue(entity.getAll()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.ANY));
            } else {
                kpis.add(new KPI().setKey(key).setValue(entity.getAll()).addAttribute(Constants.TYPE_ATTRIBUTE, Constants.ALL));
            }
            databox.push(kpis);
            LOG.info("Successfully pushed the {} data to databox", key);
        } catch (RuntimeException e) {
            throw new DataboxPushFailedException(key, e.getLocalizedMessage());
        }
    }
}
