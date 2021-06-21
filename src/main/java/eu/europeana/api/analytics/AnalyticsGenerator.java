package eu.europeana.api.analytics;

import eu.europeana.api.analytics.service.AnalyticsServiceClient;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource("classpath:analytics.properties")
@PropertySource(value = "classpath:analytics.user.properties", ignoreResourceNotFound = true)
public class AnalyticsGenerator implements CommandLineRunner{

    @Autowired
    private AnalyticsServiceClient analyticsServiceClient;

    @Override
    public void run(String... args) throws Exception {
//        if (StringUtils.isEmpty(analyticsServiceClient.getHarvestMethod())) {
//            throw new IllegalArgumentException("Please specify a harvest method (e.g. ListIdentifiers, ListRecords)");
//        }
        analyticsServiceClient.execute();
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder().sources(AnalyticsGenerator.class).web(WebApplicationType.NONE).run(args);
    }

}
