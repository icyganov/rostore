package org.rostore.service;

import org.jboss.logging.Logger;
import org.jboss.logging.MDC;
import org.rostore.service.apikey.ApiKeyRequestContext;

import jakarta.inject.Inject;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import jakarta.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.Random;

@Provider
public class RequestFilter implements ContainerRequestFilter, ContainerResponseFilter {

    private static final Logger LOG = Logger.getLogger(RequestFilter.class);

    @Inject
    RoStoreAccessor roStoreAccessor;

    @Inject
    ApiKeyRequestContext apiKeyRequestContext;

    private String generateTrackingId() {
        final Random r = new Random(System.currentTimeMillis());
        final StringBuilder sb= new StringBuilder();
        for (int i=0; i<8; i++) {
            int n = r.nextInt(36);
            if (n < 10) {
                sb.append((char)(n+48));
            } else {
                sb.append((char)(n+87));
            }
        }
        return sb.toString();
    }

    @Override
    public void filter(final ContainerRequestContext ctx) throws IOException {

        apiKeyRequestContext.init(ctx.getHeaderString(Headers.APIKEY_HEADER));
        String trackingId = ctx.getHeaderString(Headers.TRACKING_ID_HEADER);
        if (trackingId == null) {
            trackingId = generateTrackingId();
        }
        MDC.put(Headers.TRACKING_ID_HEADER, trackingId);

        LOG.infof("Request %s %s", ctx.getMethod(), ctx.getUriInfo().getPath());

        roStoreAccessor.getState().checkRequestsAllowed();
    }

    @Override
    public void filter(final ContainerRequestContext containerRequestContext, final ContainerResponseContext containerResponseContext) throws IOException {
        if (LOG.isInfoEnabled()) {
            long stopTime = System.currentTimeMillis();
            long deltaMs = stopTime - apiKeyRequestContext.startTimestamp();
            LOG.infof("Finished after %dms", deltaMs);
        }
        containerResponseContext.getHeaders().
                add(Headers.TRACKING_ID_HEADER,
                        MDC.get(Headers.TRACKING_ID_HEADER));
    }
}