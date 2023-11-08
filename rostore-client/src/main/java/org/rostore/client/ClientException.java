package org.rostore.client;

public class ClientException extends RuntimeException {

    private final String trackingId;

    private final String serverMessage;

    private final RequestProperties requestProperties;

    public String getServerMessage() {
        return serverMessage;
    }

    public RequestProperties getRequestProperties() {
        return requestProperties;
    }

    public String getTrackingId() {
        return trackingId;
    }

    public ClientException(final String message, final RequestProperties requestProperties, final String trackingId, final String serverMessage) {
        super(message);
        this.requestProperties = requestProperties;
        this.trackingId = trackingId;
        this.serverMessage = serverMessage;
    }

    public ClientException(final String message, final RequestProperties requestProperties) {
        super(message);
        this.requestProperties = requestProperties;
        this.trackingId = null;
        this.serverMessage = null;
    }

    public ClientException(final String message, final RequestProperties requestProperties, final String trackingId, final String serverMessage, final Throwable throwable) {
        super(message, throwable);
        this.requestProperties = requestProperties;
        this.trackingId = trackingId;
        this.serverMessage = serverMessage;
    }

    public ClientException(final String message, final RequestProperties requestProperties, final Throwable throwable) {
        super(message, throwable);
        this.requestProperties = requestProperties;
        this.trackingId = null;
        this.serverMessage = null;
    }
}
