package org.rostore.service;

import org.rostore.entity.Record;

import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.core.Response;

public class Headers {

    public static final String APIKEY_HEADER = "api-key";
    public static final String TTL_HEADER = "ttl";

    public static final String OPTIONS_HEADER = "options";
    public static final String VERSION_HEADER = "version";
    public static final String EOL_HEADER = "eol";
    public static final String TRACKING_ID_HEADER = "trackingId";

    public final static void toHeaders(final HttpServletResponse servletResponse, final Record record) {
        servletResponse.addHeader(Headers.EOL_HEADER, String.valueOf(record.getUnixEol()));
        servletResponse.addHeader(Headers.TTL_HEADER, String.valueOf(record.getTtl()));
        servletResponse.addHeader(Headers.VERSION_HEADER, String.valueOf(record.getVersion()));
    }

    public final static void toHeaders(final Response.ResponseBuilder rb, final Record record) {
        rb.header(Headers.EOL_HEADER, record.getUnixEol());
        rb.header(Headers.TTL_HEADER, record.getTtl());
        rb.header(Headers.VERSION_HEADER, record.getVersion());
    }

}
