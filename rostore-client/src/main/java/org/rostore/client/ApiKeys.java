package org.rostore.client;

import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.rostore.entity.apikey.ApiKeyDefinition;
import org.rostore.entity.apikey.ApiKeyPermissions;

import java.io.*;
import java.util.logging.Logger;

/**
 * Class is used to access and modify the api keys on the storage.
 */
public class ApiKeys {

    private final RoStoreClient roStoreClient;

    protected ApiKeys(final RoStoreClient roStoreClient) {
        this.roStoreClient = roStoreClient;
    }

    /**
     * Lists all api keys.
     * <p>The returning object is not complete. Check the {@link StringKeyList#isMore()} if this is the case.</p>
     * <p>Use {@link StringKeyList#getContinuationKey()} to the the continuationKey for the next call to get the next pack of keys.</p>
     * @param continuationKey is either continuation key to get the next package or null for the first call
     * @return the object is not complete, to check if more data is available use {@link StringKeyList#isMore()}
     */
    public StringKeyList listApiKeys(final String continuationKey) {
        StringBuilder builder = new StringBuilder("/admin/api-key/list");
        if (continuationKey != null) {
            builder.append("?").append("start-api-key=");
            builder.append(continuationKey);
        }
        String path = builder.toString();
        RequestProperties requestProperties = roStoreClient.create(path).contentType(ContentType.JSON.getMediaType());
        return roStoreClient.get(requestProperties, (httpResponse) ->
                roStoreClient.getJsonMapper().toObject(RoStoreClient.getInputStream(httpResponse), StringKeyList.class, "api-key-list")
        );
    }

    /**
     * Creates a new api-key and associate the permissions with it
     * @param apiKeyPermissions the api-key and associated permissions
     * @return versioned object containing permissions
     */
    public VersionedObject<String,ApiKeyPermissions> post(final VersionedObject<String,ApiKeyPermissions> apiKeyPermissions) {
        final RequestProperties rp = roStoreClient.create("/admin/api-key").
                unixEol(apiKeyPermissions.getUnixEOL()).
                contentType(ContentType.JSON.getMediaType());
        final InputStream inputStream = roStoreClient.getJsonMapper().fromObject(apiKeyPermissions.getValue(), "api-key");
        return roStoreClient.post(rp, inputStream, (response) -> toObject(response));
    }

    /**
     * Get the api-key permissions by the key
     * @param key the api-key
     * @return versioned object containing permissions
     */
    public VersionedObject<String,ApiKeyPermissions> get(final String key) {
        final RequestProperties rp = roStoreClient.
                create("/admin/api-key/" + key).
                contentType(ContentType.JSON.getMediaType());
        return roStoreClient.get(rp, (response) -> toObject(response));
    }

    /**
     * Updates given api-key
     * @param apiKeyPermissions object to update
     * @return the updated version
     */
    public VersionedObject<String,ApiKeyPermissions> put(final VersionedObject<String,ApiKeyPermissions> apiKeyPermissions) {
        final RequestProperties rp = roStoreClient.
                create("/admin/api-key/" + apiKeyPermissions.getKey()).
                unixEol(apiKeyPermissions.getUnixEOL()).
                contentType(ContentType.JSON.getMediaType());
        final InputStream inputStream = roStoreClient.getJsonMapper().fromObject(apiKeyPermissions.getValue(), "api-key");
        return roStoreClient.put(rp, inputStream, (response) -> toObject(response));
    }


    private VersionedObject<String,ApiKeyPermissions> toObject(final CloseableHttpResponse response) {
        //final Long version = RoStoreClient.getVersionHeader(response);
        final Long unixEol = roStoreClient.getEOLHeader(response);
        final ApiKeyDefinition apiKeyDefinition = roStoreClient.getJsonMapper().toObject(RoStoreClient.getInputStream(response), ApiKeyDefinition.class, "api-key");
        return VersionedObject.create(apiKeyDefinition.getKey(), apiKeyDefinition.getApiKeyPermissions(), null, unixEol);
    }
}

