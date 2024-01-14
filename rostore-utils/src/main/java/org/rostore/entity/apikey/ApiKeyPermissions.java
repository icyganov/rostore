package org.rostore.entity.apikey;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Defines permissions associated with APIKEY
 */
@Schema(description="Permissions associated with APIKEY")
public class ApiKeyPermissions {

    @Schema(description = "Permissions granted per container", example="{ \"yourStoreName\": [\"READ\", \"LIST\",\"WRITE\",\"DELETE\",\"CREATE\",\"GRANT\"] }")
    private Map<String, Set<Permission>> containerPermissions = new HashMap<>();

    @Schema(description = "Permissions granted on store", example=" [\"READ\", \"LIST\",\"WRITE\",\"DELETE\",\"CREATE\",\"GRANT\",\"SUPER\"] ")
    private Set<Permission> storePermissions = EnumSet.noneOf(Permission.class);

    public Set<Permission> getContainerPermissions(final String containerName) {
        return containerPermissions.get(containerName);
    }

    public Map<String, Set<Permission>> getContainerPermissions() {
        return containerPermissions;
    }

    public void setContainerPermissions(Map<String, Set<Permission>> containerPermissions) {
        this.containerPermissions = containerPermissions;
    }

    public void setContainerPermissions(final String containerName, Set<Permission> containerPermissions) {
        this.containerPermissions.put(containerName, containerPermissions);
    }

    public void setStorePermissions(final Set<Permission> storagePermissions) {
        this.storePermissions = storagePermissions;
    }

    public Set<Permission> getStorePermissions() {
        return storePermissions;
    }

    @Override
    public String toString() {
        return "ApiKeyPermissions{" +
                "containerPermissions=" + containerPermissions +
                ", storePermissions=" + storePermissions +
                '}';
    }
}
