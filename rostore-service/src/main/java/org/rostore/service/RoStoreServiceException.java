package org.rostore.service;

import org.rostore.entity.RoStoreException;

import javax.ws.rs.core.Response;

public class RoStoreServiceException extends RoStoreException {

    private final Response.Status status;

    public Response.Status getStatus() {
        return status;
    }

    public RoStoreServiceException(final Response.Status status, final String message) {
        super(message);
        this.status = status;
    }

    public RoStoreServiceException(final Response.Status status, final String message, final Throwable throwable) {
        super(message, throwable);
        this.status = status;
    }
}
