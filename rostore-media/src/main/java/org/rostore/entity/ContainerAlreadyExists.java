package org.rostore.entity;

/**
 * Thrown when the container name is already in use.
 */
public class ContainerAlreadyExists extends RoStoreException {

    public ContainerAlreadyExists(final String containerName) {
        super("Container \"" + containerName + "\" already exists");
    }

}