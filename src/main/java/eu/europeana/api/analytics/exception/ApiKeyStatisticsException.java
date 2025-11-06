package eu.europeana.api.analytics.exception;

/**
 * Exception class for Analytics API
 * @author srishti singh
 */
public class ApiKeyStatisticsException extends Exception {

    public ApiKeyStatisticsException(String msg) {
        super(msg);
    }

    public ApiKeyStatisticsException(String msg, Throwable t) {
        super(msg, t);
    }
}
