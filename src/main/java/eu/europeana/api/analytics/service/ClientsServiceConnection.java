package eu.europeana.api.analytics.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.europeana.api.analytics.exception.ApiKeyStatisticsException;
import eu.europeana.api.analytics.model.RegisteredClients;
import eu.europeana.api.commons.auth.AuthenticationHandler;
import eu.europeana.api.commons.http.HttpConnection;
import eu.europeana.api.commons.http.HttpResponseHandler;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;

/**
 * Class to connect to keycloak and get the Registerd clients
 * @author srishti singh
 */
public class ClientsServiceConnection {

    private static final Logger LOG = LogManager.getLogger(ClientsServiceConnection.class);

    protected final HttpConnection        clientConnection;
    private   final AuthenticationHandler auth;
    private   final String                registeredClientsUrl;
    private   final ObjectMapper          mapper;

    /**
     * Constructor
     * @param registeredClientsUrl url for the registered client endpoint
     * @param auth authentication for the url
     * @param mapper mapper to read the response
     */
    public ClientsServiceConnection(String registeredClientsUrl, AuthenticationHandler auth, ObjectMapper mapper) {
        this.auth                       = auth;
        this.registeredClientsUrl       = registeredClientsUrl;
        this.clientConnection           = new HttpConnection(true);
        this.mapper                     = mapper;
    }

    /**
     * fetches the project and internal client list
     * @return
     * @throws ApiKeyStatisticsException
     */
    public RegisteredClients getRegisteredClients() throws ApiKeyStatisticsException {
        try  {
            HttpResponseHandler rsp = clientConnection.get(registeredClientsUrl, "application/json", auth);
            int responseCode = rsp.getStatus();
            if (responseCode == HttpStatus.SC_OK) {
                return mapper.readValue(rsp.getResponse(), RegisteredClients.class);
            } else {
                String errorMessage = extractErrorMessage(rsp.getResponse());
                LOG.error("Error retrieving registered clients from Keycloak  : url-{}, code-{}, reason-{}",
                        this.registeredClientsUrl, responseCode, errorMessage);
                return null;
            }
        } catch (IOException e) {
            throw new ApiKeyStatisticsException("Error while getting response from keycloak - " + registeredClientsUrl + " " + e.getMessage(), e);
        }
    }

    private String extractErrorMessage(String json) {
        try {
            JsonNode node = mapper.readTree(json);
            if (node.has("error")) {
                JsonNode error = node.get("error");
                if (node.has("message")) {
                    return error.asText() + " " + node.get("message").asText();
                }
            return error.asText(); // by default return error as text
            }
        }catch (JsonProcessingException e) {
            LOG.error("Error processing error response from Keycloak - {}", json, e);
        }
        return null;
    }

    /**
     * Close method for the Client service connection
     */
    public void close(){
        try {
            clientConnection.close();
        } catch (IOException e) {
            LOG.error("Error closing the Keycloak connection");
        }
    }
}
