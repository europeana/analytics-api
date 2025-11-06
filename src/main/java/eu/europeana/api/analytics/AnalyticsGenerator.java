package eu.europeana.api.analytics;

import eu.europeana.api.analytics.config.AnalyticsApiConfig;
import eu.europeana.api.analytics.service.AnalyticsService;
import jakarta.annotation.Resource;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.PropertySource;

import java.time.ZonedDateTime;

@SpringBootApplication
@PropertySource("classpath:analytics.properties")
@PropertySource(value = "classpath:analytics.user.properties", ignoreResourceNotFound = true)
public class AnalyticsGenerator implements CommandLineRunner{

    private static final Logger LOG = LogManager.getLogger(AnalyticsGenerator.class);

    @Autowired
    private AnalyticsService analyticsService;

    @Resource
    private AnalyticsApiConfig analyticsApiConfig;

    @Override
    public void run(String... args) throws Exception {
        if (StringUtils.isEmpty(analyticsApiConfig.getDataboxToken())) {
            throw new IllegalArgumentException("Databox token cannot be empty!");
        }
        if (executeMonthlyUpdates()) {
            LOG.info("{} day of the Month [{}]. Executing monthly updates.", analyticsApiConfig.getDayOfMonth(), ZonedDateTime.now());
            analyticsService.executeMonthlyTask();
        }
        LOG.info("Executing daily updates......");
        analyticsService.executeDailyTask();
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder().sources(AnalyticsGenerator.class).web(WebApplicationType.NONE).run(args);
    }

    /**
     * Returns true if day of the month matches with configured day
     * @return
     */
    private boolean executeMonthlyUpdates() {
        ZonedDateTime dateTime = ZonedDateTime.now();
        return (dateTime.getDayOfMonth() == analyticsApiConfig.getDayOfMonth());
    }

}
