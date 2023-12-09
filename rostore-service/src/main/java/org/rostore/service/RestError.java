package org.rostore.service;

import org.jboss.logging.MDC;
import org.rostore.entity.*;
import org.rostore.v2.container.async.AsyncException;
import org.rostore.v2.container.async.OperationExecutionException;
import org.rostore.v2.container.async.OperationExecutionRuntimeException;

import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.core.Response;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RestError {

    private final static Logger logger = Logger.getLogger(RestError.class.getName());

    private final static Response.StatusType quotaStatusCode = quotaStatusCode();

    private final Response.StatusType status;

    private final ErrorRepresentation errorRepresentation;

    private final Exception exception;

    private final Level level;

    public Response.StatusType getStatus() {
        return status;
    }

    public ErrorRepresentation getErrorRepresentation() {
        return errorRepresentation;
    }

    public Exception getException() {
        return exception;
    }

    public Level getLevel() {
        return level;
    }

    private RestError(final Response.StatusType status, final ErrorRepresentation errorRepresentation, final Exception exception, final Level level) {
        this.status = status;
        this.errorRepresentation = errorRepresentation;
        this.exception = exception;
        this.level = level;
    }

    public static RestError convert(final Exception ex) {
        ErrorRepresentation ep = null;
        Response.StatusType status = Response.Status.INTERNAL_SERVER_ERROR;
        final Object o = MDC.get(Headers.TRACKING_ID_HEADER);
        final String trackingId = o != null ? o.toString() : "-";
        boolean unexpectedError = true;
        if (ex instanceof NotFoundException) {
            ep = new ErrorRepresentation(ex.getMessage(), trackingId);
            status = Response.Status.NOT_FOUND;
            unexpectedError = false;
        } else if (ex instanceof RoStoreServiceException) {
            ep = new ErrorRepresentation(ex.getMessage(), trackingId);
            status = ((RoStoreServiceException) ex).getStatus();
            unexpectedError = false;
        } else if (ex instanceof OperationExecutionRuntimeException || ex instanceof OperationExecutionException || ex instanceof AsyncException) {
            final Throwable thr;
            if (ex instanceof OperationExecutionRuntimeException || ex instanceof AsyncException) {
                thr = ex.getCause().getCause();
            } else {
                thr = ex.getCause();
            }
            ep = new ErrorRepresentation(thr.getMessage(), trackingId);
            if (thr instanceof RoStoreServiceException) {
                status = ((RoStoreServiceException) thr).getStatus();
            }
            if (thr instanceof QuotaExceededException) {
                status = quotaStatusCode;
            } else if (thr instanceof VersionMismatchInitException ||
                    thr instanceof VersionMismatchException ||
                    thr instanceof ContainerAlreadyExists) {
                status = Response.Status.CONFLICT;
                unexpectedError = false;
            } else if (thr instanceof EOLIncorrectException) {
                status = Response.Status.BAD_REQUEST;
            }
        } else if (ex instanceof EOLIncorrectException) {
            ep = new ErrorRepresentation(ex.getMessage(), trackingId);
            status = Response.Status.BAD_REQUEST;
        }
        if (ep == null) {
            ep = new ErrorRepresentation(ex.getMessage(), trackingId);
        }
        Level level;
        if (unexpectedError) {
            level = Level.SEVERE;
            logger.log(Level.SEVERE, ep.getMessage(), ex);
        } else {
            level = Level.INFO;
            logger.log(Level.INFO, ep.getMessage());
        }
        return new RestError(status, ep, ex, level);
    }

    private static Response.StatusType quotaStatusCode() {
        return new Response.StatusType() {
            @Override
            public int getStatusCode() {
                return 507;
            }

            @Override
            public Response.Status.Family getFamily() {
                return Response.Status.Family.SERVER_ERROR;
            }

            @Override
            public String getReasonPhrase() {
                return "Insufficient Storage";
            }
        };
    }
}
