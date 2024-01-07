package org.rostore.v2.container;

import org.rostore.entity.media.ContainerListProperties;

/**
 * This class is serialized and stored in the media header at the time of {@link org.rostore.v2.media.Media} creation.
 * <p>This data can't be changed after the media is created.</p>
 */
public class ContainerListHeader {

    private long keyStartIndex;

    private ContainerListProperties containerListProperties;

    /**
     * The start block index, the index where the container key sequence is stored.
     * <p>It is assigned when the storage is created and stays permanent.</p>
     *
     * @return the index of container's list first block
     */
    public long getKeyStartIndex() {
        return keyStartIndex;
    }

    /**
     * Assigned at the media creation.
     *
     * @param keyStartIndex the first block of the container's list
     */
    public void setKeyStartIndex(long keyStartIndex) {
        this.keyStartIndex = keyStartIndex;
    }

    /**
     * The list container's list properties defined at the creation phase, which steers the container's list properties
     *
     * @return the properties of container's list
     */
    public ContainerListProperties getContainerListProperties() {
        return containerListProperties;
    }

    public ContainerListHeader(final ContainerListProperties containerListProperties) {
        this.containerListProperties = containerListProperties;
    }
}
