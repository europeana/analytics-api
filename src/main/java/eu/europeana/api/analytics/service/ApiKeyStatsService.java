package eu.europeana.api.analytics.service;

import java.io.*;
import java.util.*;

import com.jayway.jsonpath.JsonPath;
import eu.europeana.api.analytics.config.AnalyticsApiConfig;
import eu.europeana.api.analytics.exception.ApiKeyStatisticsException;
import eu.europeana.api.analytics.model.ApiKeyStats;
import eu.europeana.api.analytics.model.RegisteredClients;
import eu.europeana.api.commons.definitions.statistics.user.ELKMetric;
import jakarta.annotation.Resource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Api key statistics service
 * @author srishti singh
 */
@Service
public class ApiKeyStatsService {

    private static final Logger LOG           = LogManager.getLogger(ApiKeyStatsService.class);
    private static final int DEFAULT_SIZE     = 200;

    @Resource
    private AnalyticsApiConfig analyticsApiConfig;
    private ElasticSearchConnection elasticSearchConnection;
    private ClientsServiceConnection clientsServiceConnection;

    @Autowired
    ApiKeyStatsService(ElasticSearchConnection elasticSearchConnection, ClientsServiceConnection clientsServiceConnection) {
        this.elasticSearchConnection = elasticSearchConnection;
        this.clientsServiceConnection = clientsServiceConnection;
    }

    /**
     * Generates the Metric by fetching stats from ELK and keycloak.
     *
     * @return elk metric
     * @throws IOException
     * @throws ApiKeyStatisticsException exceptions during stats generation
     */
    public ELKMetric generate() throws ApiKeyStatisticsException {
        LOG.info("Fetching from keycloak registered client list ...... ");
        RegisteredClients registeredClients = clientsServiceConnection.getRegisteredClients();
        List<ApiKeyStats> list = getStats();


        if (!list.isEmpty() && registeredClients != null) {
            LOG.info(" Apikey stats accumulated for this month - {} ", list.size());

            // calculate metrics
            int totalInternalTraffic = getInternalClientUsage(list, registeredClients.getInternal());
            int totalExternalTraffic = getExternalClientUsage(list, registeredClients.getInternal());

            removeInternalClients(list, registeredClients.getInternal());

            LOG.info(" Apikey stats after removing internal clients - {} ", list.size());

            List<ApiKeyStats> projectClients  = new ArrayList<>();
            List<ApiKeyStats> personalClients = new ArrayList<>();

            fetchProjectAndPersonalClients(list, registeredClients.getProjects(), projectClients, personalClients);

            int regularCustomer =  getRegular(projectClients, "project");
            int regularUser = getRegular(personalClients, "personal");

            ELKMetric elkMetric =  new ELKMetric(regularCustomer, projectClients.size(),
                    regularUser, personalClients.size(),
                    list.size(),
                    totalExternalTraffic,
                    totalInternalTraffic);

            LOG.info("Apikey usage stats :: \n {}", elkMetric);
            return  elkMetric;
        } else {
            LOG.info("Not generating monthly statistics. Either registered clients is null or elk stats .... ");
        }
        return null;
    }


    /**
     * Fetches the Statistics from ELK
     * @return
     * @throws ApiKeyStatisticsException
     * @throws IOException
     */
    @SuppressWarnings("java:S3740") // the data coming from ELK is in complex format, can not define the specific type
    private List<ApiKeyStats> getStats() throws ApiKeyStatisticsException {
        LOG.info("Fetching monthly apikey usage statistics from ELK ...... ");
        String json = elasticSearchConnection.getApiKeyData();
        if (json != null) {
            Map apikeys = JsonPath.read(json, "$.aggregations.apikeys");
            List<String> dates = JsonPath.read(apikeys, "$.buckets[*].date.buckets[*].key_as_string");
            List<Map> buckets = JsonPath.read(apikeys, "$.buckets");

            List<ApiKeyStats> list = new ArrayList(DEFAULT_SIZE);

            Set<String> sDates = new TreeSet<>(dates);
            for (Map bucket : buckets) {
                list.add(getStats(bucket, sDates));
            }
            printCSV(list, sDates, analyticsApiConfig.getApiKeyAndDatesCsvFile());
            return list;
        }
        return Collections.emptyList();
    }


    /**
     * Returns the maximum of activeKeysPerMonth and activeKeysPerDay for the list of statistics provided
     * @param list stats
     * @param message message for logs
     * @return
     */
    private int getRegular(List<ApiKeyStats> list, String message) {
        int activeKeysPerMonth = getActiveKeysPerMonth(list);
        int activeKeysPerDay   = getActiveKeysPerDay(list);
        LOG.info("For {} clients - Active Keys Per Month : {}, Active Keys Per Day : {}", message, activeKeysPerMonth, activeKeysPerDay);
        return Math.max(activeKeysPerDay, activeKeysPerMonth);
    }


    /**
     * Removes the internal clients if present in this month statistics
     * @param list apikey statistics for this month
     * @param internalClients list of internal clients
     */
    private void removeInternalClients(List<ApiKeyStats> list, List<String> internalClients) {
        List<ApiKeyStats> internalClientStats = list.stream().filter(
                key -> internalClients.stream().anyMatch(
                        internal -> internal.equals(key.getApiKey()))).toList();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing internal clients stats for - {} ", internalClientStats.stream().map(ApiKeyStats::getApiKey).toList());
        }
        list.removeAll(internalClientStats); // remove internal stats
    }

    /**
     * Fetches the project and personal clients statistics from this month stats.
     * @param list apikey statistics for this month
     * @param projects list of project clients
     * @param projectClients list to be updated for with project clients only
     * @param personalClients list to be updated for with personal clients only
     */
    private void fetchProjectAndPersonalClients(List<ApiKeyStats> list, List<String> projects,
                                                List<ApiKeyStats> projectClients, List<ApiKeyStats> personalClients) {
        for (ApiKeyStats stat: list) {
            if (projects.contains(stat.getApiKey())) {
                projectClients.add(stat);
            } else {
                personalClients.add(stat);
            }
        }
    }

    @SuppressWarnings("java:S3740") // the data coming from ELK is in complex format, can not define the specific type
    private ApiKeyStats getStats(Map bucket, Collection<String> dates) {
        int total  = 0;
        int active = 0;
        ApiKeyStats apikey = new ApiKeyStats((String)bucket.get("key"));
        for ( String date : dates ) {
            List b = JsonPath.read(bucket, "$.date.buckets[?(@.key_as_string=='" + date + "')]");
            int count = ( b.isEmpty() ? 0 : (Integer)((Map)b.get(0)).get("doc_count") );
            apikey.put(date, count);

            active += (count >= analyticsApiConfig.getCallsPerDay() ? 1 : 0);
            total  += count;
        }
        apikey.setTotal(total);
        apikey.setActive(active);
        return apikey;
    }

    /**
     * prints the Apikey and dates summary in a csv file
     * @param list apikey statistics list
     * @param dates dates of the apikey stats
     * @throws IOException
     */
    private void printCSV(List<ApiKeyStats> list, Collection<String> dates, File file) throws ApiKeyStatisticsException {
        if (file != null) {
            try (PrintStream ps = new PrintStream(file, "UTF-8")) {
                printCSVHeader(dates, ps);
                for (ApiKeyStats stat : list) {
                    printApiKeyCSV(stat, dates, ps);
                }
            } catch (IOException e) {
                throw new ApiKeyStatisticsException(" Error generating apikey dates CSV file -  " + e.getMessage(), e);
            }
        } else {
            LOG.info("Not generating Apikey/dates csv file. Target csv file not provided");
        }
    }

    private void printCSVHeader(Collection<String> dates, PrintStream ps) {
        ps.print("APIKEY");
        for ( String date : dates ) {
            ps.print(",");
            ps.print(date);
        }
        ps.print(",Total,Active Days");
        ps.println();
    }

    /**
     * prints the Apikey and dates summary in a csv file
     * @param stat apikey statistics
     * @param dates dates of the apikey stats
     * @throws IOException
     */
    private void printApiKeyCSV(ApiKeyStats stat, Collection<String> dates, PrintStream ps) {
        ps.print(stat.getApiKey());
        for ( String date : dates ) {
            ps.print(",");
            ps.print(stat.get(date));
        }
        ps.print(",");
        ps.print(stat.getTotal());
        ps.print(",");
        ps.print(stat.getActive());
        ps.println();
    }

    /**
     * Returns the Active keys per day
     * @param list apikey statistics list
     * @return
     */
    private int getActiveKeysPerDay(List<ApiKeyStats> list) {
        int count = 0;
        for ( ApiKeyStats stat : list ) {
            count += (stat.getActive() >= analyticsApiConfig.getActiveDays() ? 1 : 0);
        }
        return (count - 1);
    }

    /**
     * Returns the Active keys per month
     * @param list apikey statistics list
     * @return
     */
    private int getActiveKeysPerMonth(List<ApiKeyStats> list) {
        int count = 0;
        int days  = list.get(0).size();
        int thold = days * analyticsApiConfig.getCallsPerDay();
        for ( ApiKeyStats stat : list ) {
            count += (stat.getTotal() >= thold ? 1 : 0);
        }
        return (count - 1);
    }

    /**
     * Fetches the internal client usage for this month
     * @param list Apikey statistics from ELK
     * @param internalClients internal client list
     * @return total usage of internal clients
     */
    private int getInternalClientUsage(List<ApiKeyStats> list, List<String> internalClients) {
        int traffic = 0;
        for(ApiKeyStats stats : list) {
            if (internalClients.contains(stats.getApiKey())) {
                traffic += stats.getTotal();
            }
        }
        return traffic;
    }

    /**
     * Fetches the external client usage for this month
     * @param list Apikey statistics from ELK
     * @param internalClients internal client list to be excluded from the usage total
     * @return total usage of external clients
     */
    private int getExternalClientUsage(List<ApiKeyStats> list, List<String> internalClients) {
        int traffic = 0;
        for(ApiKeyStats stats : list) {
            if (!internalClients.contains(stats.getApiKey())) {
                traffic += stats.getTotal();
            }
        }
        return traffic;
    }

}
