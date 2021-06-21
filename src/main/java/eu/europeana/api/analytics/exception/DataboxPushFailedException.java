package eu.europeana.api.analytics.exception;

public class DataboxPushFailedException extends Exception {

    public DataboxPushFailedException(String type, String msg) {
        super("Error pushing metric data to DataBox for " + type + ". " + msg);
    }
}
