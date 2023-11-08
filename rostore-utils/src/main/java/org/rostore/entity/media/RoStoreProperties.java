package org.rostore.entity.media;

import org.eclipse.microprofile.openapi.annotations.media.Schema;

public class RoStoreProperties {

    @Schema(description = "Media properties", example="{ \"maxTotalSize\": 1073741824, \"blockSize\": 4096, \"closeUnusedBlocksAfterMillis\": 5000, \"closeUnusedSequencesAfterMillis\": 10000}")
    private MediaPropertiesBuilder mediaProperties = new MediaPropertiesBuilder();

    @Schema(description = "Container list properties", example="{ \"autoCloseContainersAfterMillis\": 60000, \"cleanupIntervalMillis\": 5000, \"maxCleanupsPerCycle\": 5, \"maxContainersPerList\": 1024, \"maxContainersListSize\": 5242880, \"maxKeyOperationsPerShard\": 10}")
    private ContainerListProperties containerListProperties = new ContainerListProperties();

    public MediaPropertiesBuilder getMediaProperties() {
        return mediaProperties;
    }

    public void setMediaProperties(MediaPropertiesBuilder mediaProperties) {
        this.mediaProperties = mediaProperties;
    }

    public ContainerListProperties getContainerListProperties() {
        return containerListProperties;
    }

    public void setContainerListProperties(ContainerListProperties containerListProperties) {
        this.containerListProperties = containerListProperties;
    }
}
