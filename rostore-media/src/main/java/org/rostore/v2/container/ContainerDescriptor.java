package org.rostore.v2.container;

import org.rostore.entity.media.ContainerMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is created on the container creation event and should be stored in the
 * container list, so it can be reopened again.
 *
 * <p>It contains all the information about he container internal structure,
 * as well as the metadata provided at the creation phase.</p>
 * <p>This class is persisted and should only be changed only on the
 * new storages, as it would change the persistence structure.</p>
 */
public class ContainerDescriptor {

    private ContainerMeta containerMeta;

    private List<ContainerShardDescriptor> shardDescriptors;

    public ContainerDescriptor(final ContainerMeta containerMeta) {
        this.containerMeta = containerMeta;
    }

    /**
     * Needed for the load
     */
    public ContainerDescriptor() {}

    /**
     * The metadata provided at the container creation
     * @return the metadata of the container
     */
    public ContainerMeta getContainerMeta() {
        return containerMeta;
    }

    public void setContainerMeta(ContainerMeta containerMeta) {
        this.containerMeta = containerMeta;
    }

    public List<ContainerShardDescriptor> getShardDescriptors() {
        if (shardDescriptors == null) {
            shardDescriptors = new ArrayList<>();
        }
        return shardDescriptors;
    }
}
