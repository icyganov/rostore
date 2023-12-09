package org.rostore.service.apikey;

import org.rostore.service.RoStoreServiceException;

import jakarta.ws.rs.core.Response;

public class PermissionDeniedException extends RoStoreServiceException {
    public PermissionDeniedException(final String message) {
        super(Response.Status.FORBIDDEN, message);
    }
}
