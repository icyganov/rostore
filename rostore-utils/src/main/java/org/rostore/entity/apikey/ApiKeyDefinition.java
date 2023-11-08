package org.rostore.entity.apikey;

import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.rostore.entity.apikey.ApiKeyPermissions;

@Schema(description = "Api Key")
public class ApiKeyDefinition {

    @Schema(description = "The API KEY itself", example = "4e030824-08bf-4a0a-b6cb-bafa19406349")
    private String key;
    @Schema(description = "Unix Timestamp for the last update")
    private long lastUpdate;
    @Schema(description = "Set of permissions managed for this key")
    private ApiKeyPermissions apiKeyPermissions;

    public ApiKeyDefinition() {

    }

    public ApiKeyDefinition(String key, ApiKeyPermissions apiKeyPermissions) {
        this.lastUpdate = System.currentTimeMillis();
        this.key = key;
        this.apiKeyPermissions = apiKeyPermissions;
    }

    public String getKey() {
        return key;
    }

    public ApiKeyPermissions getApiKeyPermissions() {
        return apiKeyPermissions;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    @Override
    public String toString() {
        return "ApiKeyDefinition{" +
                "key='" + key + '\'' +
                ", lastUpdate=" + lastUpdate +
                ", apiKeyPermissions=" + apiKeyPermissions +
                '}';
    }
}
