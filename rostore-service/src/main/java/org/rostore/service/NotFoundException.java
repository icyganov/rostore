package org.rostore.service;

import jakarta.ws.rs.core.Response;

public class NotFoundException extends RoStoreServiceException {
    public NotFoundException(final String message) {
        super(Response.Status.NOT_FOUND, message);
    }
}
