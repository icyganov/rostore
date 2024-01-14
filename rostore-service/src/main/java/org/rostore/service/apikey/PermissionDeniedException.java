package org.rostore.service.apikey;

import org.rostore.service.RoStoreServiceException;

import jakarta.ws.rs.core.Response;

/**
 * Exception is thrown when the client tries to execute an operation
 * that it has no sufficient permissions for.
 */
public class PermissionDeniedException extends RoStoreServiceException {
    public PermissionDeniedException(final String message) {
        super(Response.Status.FORBIDDEN, message);
    }
}
