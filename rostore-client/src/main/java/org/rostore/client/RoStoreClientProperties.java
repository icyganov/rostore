package org.rostore.client;

import java.time.Duration;

public class RoStoreClientProperties {

    private final static int UPDATE_RETRIES = 10;

    private final static int MAX_CONNECTIONS_TOTAL = 200;

    private final static int MAX_CONNECTIONS_PER_ROUTE = MAX_CONNECTIONS_TOTAL;

    private final static Duration UPDATE_TIMEOUT_MAX = Duration.ofMillis(1000);

    private final static Duration REQUEST_TIMEOUT = Duration.ofMinutes(1);

    private final static Duration CONNECT_TIMEOUT = Duration.ofMinutes(2);

    private int updateRetries = UPDATE_RETRIES;
    private Duration updateTimeoutMax = UPDATE_TIMEOUT_MAX;

    private Duration requestTimeout = REQUEST_TIMEOUT;

    private Duration connectTimeout = CONNECT_TIMEOUT;

    private int maxTotalConnections = MAX_CONNECTIONS_TOTAL;

    private int maxConnectionsPerRoute = MAX_CONNECTIONS_PER_ROUTE;

    private final String apiKey;

    private final String baseUrl;

    public int getUpdateRetries() {
        return updateRetries;
    }

    public void setUpdateRetries(int updateRetries) {
        this.updateRetries = updateRetries;
    }

    public Duration getUpdateTimeoutMax() {
        return updateTimeoutMax;
    }

    public void setUpdateTimeoutMax(Duration updateTimeoutMax) {
        this.updateTimeoutMax = updateTimeoutMax;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    /**
     * Returns the api key used to access the rostore
     * @return the api key
     */
    public String getApiKey() {
        return apiKey;
    }

    /**
     * Returns the base url of the rostore instance, e.g. https://ro-store.net or http://localhost:8080. All the paths starts immediately after the base url
     * @return the base url
     */
    public String getBaseUrl() {
        return baseUrl;
    }

    /**
     *
     * @return
     */
    public Duration getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Duration connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getMaxTotalConnections() {
        return maxTotalConnections;
    }

    public void setMaxTotalConnections(int maxTotalConnections) {
        this.maxTotalConnections = maxTotalConnections;
    }

    public int getMaxConnectionsPerRoute() {
        return maxConnectionsPerRoute;
    }

    public void setMaxConnectionsPerRoute(int maxConnectionsPerRoute) {
        this.maxConnectionsPerRoute = maxConnectionsPerRoute;
    }

    public RoStoreClientProperties(final String baseUrl, final String apiKey) {
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
    }
}
