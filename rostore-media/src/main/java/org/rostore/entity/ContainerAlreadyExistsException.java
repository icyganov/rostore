package org.rostore.entity;

/**
 * Thrown when the container name is already in use.
 */
public class ContainerAlreadyExistsException extends RoStoreException {

    /**
     * Creates an exception for the provided container
     *
     * @param containerName the name of the container
     */
    public ContainerAlreadyExistsException(final String containerName) {
        super("Container \"" + containerName + "\" already exists");
    }

}