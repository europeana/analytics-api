package eu.europeana.api.analytics.utils;

import eu.europeana.api.commons.definitions.statistics.UsageStatsFields;

public class Constants extends UsageStatsFields {

    // Gallery DataBox Constants
    public static final String GALLERY_METRICS               = "GalleryMetrics";
    public static final String TYPE_ATTRIBUTE                = "Type";
    public static final String PUBLIC_SETS                   = "PublicSets";
    public static final String PRIVATE_SETS                  = "PrivateSets";
    public static final String ITEMS_LIKED                   = "ItemsLiked";

    // Entity DataBox Constants
    public static final String ENTITY_TYPE_METRICS           = "EntityPerType";
    public static final String ENTITY_ATTRIBUTE_LANG         = "Language";
    public static final String TIMESPAN                      = "Timespan";
    public static final String PLACE                         = "Place";
    public static final String ORGANISATION                  = "Organization";
    public static final String CONCEPT                       = "Concept";
    public static final String AGENT                         = "Agent";
    public static final String TOTAL                         = "Total";

    // Beans
    public static final String ELASTIC_SEARCH_CONNECTION   = "elasticSearchConnection";
    public static final String REGISTERED_CLIENT_CONNECTION= "registeredClientConnection";

    public static final String DATABOX                     = "databox";

    // error constants
    public static final String ERROR                      = "error";
    public static final String REASON                     = "reason";
    public static final String ROOT_CAUSE                 = "root_cause";

}
