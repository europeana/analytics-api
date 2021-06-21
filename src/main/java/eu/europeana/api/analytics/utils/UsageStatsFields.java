package eu.europeana.api.analytics.utils;

// TODO - Remove this class and use the set-api:set-usage-Stats Metric class
//  once set-api is uploaded in artifactory.
public class UsageStatsFields {

    UsageStatsFields() {
    }

    public static final String TYPE               = "type";
    public static final String CREATED            = "created";

    public static final String PRIVATE_SETS       = "NumberOfPrivateSets";
    public static final String PUBLIC_SETS        = "NumberOfPublicSets";
    public static final String ITEMS_LIKED        = "NumberOfItemsLiked";
    public static final String SETS_PER_USER      = "AverageSetsPerUser";
}
