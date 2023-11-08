package org.rostore.v2.container;

import org.rostore.entity.media.ContainerMeta;

import java.util.ArrayList;
import java.util.List;

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
