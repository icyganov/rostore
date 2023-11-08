package org.rostore.entity;

public class ContainerAlreadyExists extends RoStoreException {

    public ContainerAlreadyExists(final String containerName) {
        super("Container \"" + containerName + "\" already exists");
    }

}