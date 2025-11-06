package eu.europeana.api.analytics.exception;

/**
 * Exception for Databox failures
 * @author srishti singh
 */
public class DataboxPushFailedException extends Exception {


    public DataboxPushFailedException(String type, String msg) {
        super("Error pushing metric data to DataBox for " + type + ". " + msg);
    }

    /**
     * Exception with throwable
     * @param type type of the attribute
     * @param msg message
     * @param t throwable
     */
    public DataboxPushFailedException(String type, String msg, Throwable t) {
        super("Error pushing metric data to DataBox for " + type + ". " + msg, t);
    }

    public DataboxPushFailedException(String msg) {
        super("Error pushing metric data to DataBox." + msg);
    }
}
