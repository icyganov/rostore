package org.rostore.v2.container;

import org.rostore.entity.media.ContainerListProperties;

public class ContainerListHeader {

    private long keyStartIndex;

    private ContainerListProperties containerListProperties;

    public long getKeyStartIndex() {
        return keyStartIndex;
    }

    public void setKeyStartIndex(long keyStartIndex) {
        this.keyStartIndex = keyStartIndex;
    }

    public ContainerListProperties getContainerListProperties() {
        return containerListProperties;
    }

    public ContainerListHeader(final ContainerListProperties containerListProperties) {
        this.containerListProperties = containerListProperties;
    }

    public ContainerListHeader() {

    }
}
