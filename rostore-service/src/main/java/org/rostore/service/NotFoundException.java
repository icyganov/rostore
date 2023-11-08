package org.rostore.service;

import javax.ws.rs.core.Response;

public class NotFoundException extends RoStoreServiceException {
    public NotFoundException(final String message) {
        super(Response.Status.NOT_FOUND, message);
    }
}
