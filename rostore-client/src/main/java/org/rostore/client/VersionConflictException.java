package org.rostore.client;

/**
 * Exception is thrown on post/put when the version in e input is not as expected by the server.
 *
 * <p>Client should provide the version in the request header as it has received in the previous get/put/post transaction.
 * If the server recognizes that the object has been changed in mean time, i.e. the version on server is higher than the one in the request.
 * </p>
 */
public class VersionConflictException extends ClientException {
    public VersionConflictException(final String message, final RequestProperties requestProperties) {
        super(message, requestProperties);
    }
}
