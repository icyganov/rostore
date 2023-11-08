package org.rostore.service;

import org.jboss.logging.MDC;

public class ErrorRepresentation {

    private final String message;
    private final String trackingId;

    public ErrorRepresentation(final String message, final String trackingId) {
        this.message = message;
        this.trackingId = trackingId;
    }

    public String getMessage() {
        return message;
    }

    public String getTrackingId() {
        return trackingId;
    }
}
