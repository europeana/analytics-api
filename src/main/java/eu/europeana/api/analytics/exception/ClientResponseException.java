package eu.europeana.api.analytics.exception;

public class ClientResponseException extends Exception{

    public ClientResponseException(String url, String msg) {
        super("Error fetching response from  " + url + ". " + msg);
    }

    public ClientResponseException(String msg) {
        super(msg);
    }
}
