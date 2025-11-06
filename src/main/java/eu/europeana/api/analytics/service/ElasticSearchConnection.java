package eu.europeana.api.analytics.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.api.analytics.exception.ApiKeyStatisticsException;
import eu.europeana.api.commons.http.HttpConnection;
import eu.europeana.api.commons.http.HttpResponseHandler;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static eu.europeana.api.analytics.utils.Constants.ERROR;
import static eu.europeana.api.analytics.utils.Constants.REASON;
import static eu.europeana.api.analytics.utils.Constants.ROOT_CAUSE;

import java.io.IOException;

/**
 *  class to connect to Elastic serach
 * @author srishti singh
 */
public class ElasticSearchConnection {

    private static final Logger LOG = LogManager.getLogger(ElasticSearchConnection.class);

    protected final HttpConnection esClient;
    private   final String         esUrl;
    private   final String         requestBody;
    private   final ObjectMapper   mapper;

    /**
     * Constructor
     * @param esUrl url for the Elastic search endpoint
     * @param requestBody request body
     * @param mapper mapper to read the response
     */
    public ElasticSearchConnection(String esUrl, String requestBody, ObjectMapper mapper) {
        this.esUrl       = esUrl;
        this.requestBody = requestBody;
        this.esClient    = new HttpConnection(true);
        this.mapper      =  mapper   ;
    }

    public String getApiKeyData() throws ApiKeyStatisticsException {
        try  {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Requesting Elastic Search - {} with body {}", esUrl, requestBody);
            }
            HttpResponseHandler rsp = esClient.post(esUrl, requestBody, "application/json", null);
            int responseCode = rsp.getStatus();
            if (responseCode == HttpStatus.SC_OK) {
                return rsp.getResponse();
            } else {
                String message = extractErrorMessage(rsp.getResponse());
                LOG.error("Error retrieving data from Elastic search : url-{}, code-{}, reason-{}",
                        this.esUrl, responseCode, message );
                return null;
            }
        } catch (IOException e) {
            throw new ApiKeyStatisticsException("Error while getting response from Elastic search url - " + esUrl + " " + e.getMessage(), e);
        }
    }

    /**
     * Will extract the error response reason phrase from elastic search error response
     * @param json error response
     * @return error message
     */
    private String extractErrorMessage(String json) {
        try {
            JsonNode node = mapper.readTree(json);
            if (node.has(ERROR)) {
                return getReasonOrCause(node.get(ERROR));
            }
        } catch (JsonProcessingException e) {
            LOG.error("Error processing error response from Elastic search - {}", json, e);
        }
        return null;
    }

    private String getReasonOrCause(JsonNode errorNode){
        if (errorNode.has(REASON)) {
            return errorNode.get(REASON).asText();
        }
        if (errorNode.has(ROOT_CAUSE)) {
            JsonNode rootCauseNode = errorNode.get(ROOT_CAUSE);
            if (rootCauseNode.isArray()) {
                return rootCauseNode.get(0).get(REASON).asText();
            }
        }
        return errorNode.asText(); // by default return error as text
    }

    /**
     * Close method for the Elastic search Connection
     */
    public void close(){
        try {
            esClient.close();
        } catch (IOException e) {
           LOG.error("Error closing the Elastic search client connection");
        }
    }
}
