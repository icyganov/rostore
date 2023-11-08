package org.rostore.service;

import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.logging.Level;
import java.util.logging.Logger;

@Provider
@Produces(MediaType.APPLICATION_JSON)
public class ErrorHandler implements ExceptionMapper<Exception> {

    private final static Logger logger = Logger.getLogger(ErrorHandler.class.getName());

    @Override
    @Produces(MediaType.APPLICATION_JSON)
    public Response toResponse(final Exception ex) {
        final RestError restError = RestError.convert(ex);
        if (Level.INFO.equals(restError.getLevel())) {
            logger.log(Level.INFO, restError.getErrorRepresentation().getMessage());
        } else {
            logger.log(restError.getLevel(), restError.getErrorRepresentation().getMessage(), ex);
        }
        return Response.status(restError.getStatus()).header("Content-Type", MediaType.APPLICATION_JSON).entity(restError.getErrorRepresentation()).build();
    }
}