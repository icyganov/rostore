package org.rostore.client;

public class ClientContainerException extends RuntimeException {

    private final String method;
    private final String container;
    private final String key;

    public String getMethod() {
        return method;
    }

    public String getContainer() {
        return container;
    }

    public String getKey() {
        return key;
    }

    public ClientContainerException(String message, String method, String container, String key, final Exception e) {
        super(message, e);
        this.method = method;
        this.container = container;
        this.key = key;
    }
}
