package org.rostore.v2.container;

import org.rostore.entity.*;
import org.rostore.entity.Record;
import org.rostore.entity.media.ContainerListProperties;
import org.rostore.entity.media.ContainerMeta;
import org.rostore.entity.media.RecordOption;
import org.rostore.v2.data.DataReader;
import org.rostore.v2.data.DataWriter;
import org.rostore.v2.keys.KeyBlockOperations;
import org.rostore.v2.keys.KeyList;
import org.rostore.v2.keys.RecordLengths;
import org.rostore.v2.media.Media;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This is the major class to manage the containers on the rostore.
 * <p>Note that containers are optional components are are not stored on the {@link Media} per default.</p>
 * <p>If containers are required, this class should be created and made persisted on the media. For
 * this the header stored on the media can be modified.</p>
 * <p>To create the containers on the media the {@link #ContainerListOperations(Media, ContainerListProperties)} should be executed.</p>
 * <p>This method will create the {@link ContainerListHeader}, which should be persisted on the {@link Media}.</p>
 * <p>When the {@link Media} is opened, the {@link ContainerListOperations} can be recreated again if the
 * stored {@link ContainerListHeader} is provided to the {@link #ContainerListOperations(Media, ContainerListHeader)},
 * which will reopen the containers in the form as they've been persisted.</p>
 */
public class ContainerListOperations implements AutoCloseable {

    private final ContainerListHeader containerListHeader;

    private final KeyBlockOperations keyBlockOperations;

    private Map<String, Container> openedContainers = new HashMap<>();

    public ContainerListHeader getContainerListHeader() {
        return containerListHeader;
    }

    /**
     * Use {@link Container#close()} to close the containers.
     * <p>This function is involved in irs operation and removes the container from the list,
     * and allows to block the list during this operation.</p>
     * @param containerName the name of the container to be closed
     */
    protected void closeContainer(final String containerName) {
        Container container = openedContainers.get(containerName);
        if (container != null) {
            synchronized (this) {
                container = openedContainers.get(containerName);
                if (container != null) {
                    container.closeInternal();
                    openedContainers.remove(containerName);
                } else {
                    throw new RoStoreException("Container " + containerName + " is not open.");
                }
            }
        } else {
            throw new RoStoreException("Container " + containerName + " is not open.");
        }
    }

    @Override
    public void close() {
        for(Map.Entry<String, Container> entry : openedContainers.entrySet()) {
            entry.getValue().close();
        }
        openedContainers.clear();
    }

    public Container get(final String containerName) {
        return getOrExecute(containerName, () -> {
            try {
                final Record record = keyBlockOperations.getRecord(containerName.getBytes(StandardCharsets.UTF_8));
                if (record == null) {
                    return null;
                }
                final ContainerDescriptor containerDescriptor = DataReader.readObject(getMedia(), record.getId(), ContainerDescriptor.class);
                return new Container(this, containerName, containerDescriptor);
            } finally {
                keyBlockOperations.commit();
            }
        }, null);
    }


    public synchronized BlockAllocation getMemoryManagement() {
        throw new RoStoreException("Not implemented yet...");
    }

    public synchronized boolean remove(final String containerName) {
        Container container = get(containerName);
        if (container == null) {
            return false;
        }
        container.remove();
        openedContainers.remove(containerName);
        try {
            final Record record = new Record();
            keyBlockOperations.remove(containerName.getBytes(StandardCharsets.UTF_8), record);
            try (final DataReader dr = DataReader.open(getMedia().getRootBlockAllocator(), record.getId())) {
                dr.free();
            }
        } finally {
            keyBlockOperations.commit();
        }
        return true;
    }

    public synchronized List<String> listAllContainers() {
        final KeyList keyList = keyBlockOperations.list(null, null, containerListHeader.getContainerListProperties().getMaxContainersPerList(), containerListHeader.getContainerListProperties().getMaxContainersListSize());
        if (keyList.isMore()) {
            throw new RoStoreException("There are too many (max number=" + containerListHeader.getContainerListProperties().getMaxContainersPerList() + ", max size =" + containerListHeader.getContainerListProperties().getMaxContainersListSize() + "B) containers.");
        }
        return keyList.getKeys().stream().map(f->new String(f, StandardCharsets.UTF_8)).collect(Collectors.toList());
    }

    public Media getMedia() {
        return keyBlockOperations.getBlockSequence().getBlockProvider().getMedia();
    }

    public Container create(final String containerName, final ContainerMeta containerMeta) {
        return getOrExecute(containerName, () -> {
                try {
                    Record record = keyBlockOperations.getRecord(containerName.getBytes(StandardCharsets.UTF_8));
                    if (record != null) {
                        throw new ContainerAlreadyExists(containerName);
                    }
                    // the next could crash in case no space available
                    try {
                        final Container container = new Container(this, containerName, containerMeta);
                        try {
                            long id = DataWriter.writeObject(getMedia().getRootBlockAllocator(), container.getDescriptor());
                            record = new Record().id(id).addOption(RecordOption.ONLY_INSERT);
                            keyBlockOperations.put(containerName.getBytes(StandardCharsets.UTF_8), record);
                            return container;
                        } catch (final Exception e) {
                            try {
                                if (record != null) {
                                    try (final DataReader dr = DataReader.open(getMedia().getRootBlockAllocator(), record.getId())) {
                                        dr.free();
                                    }
                                }
                            } finally {
                                container.remove();
                            }
                            throw e;
                        }
                    } catch (final Exception e) {
                        throw new RoStoreException("Error while creating a new container \"" + containerName + "\".", e);
                    }
                } finally {
                    keyBlockOperations.commit();
                }
            }, (container) -> {
                throw new RoStoreException("The container <" + containerName + "> already exists and opened.");
            });
    }

    /**
     * Should only be used internally
     * @param name
     */
    public void evict(final String name) {
        openedContainers.remove(name);
    }

    private Container getOrExecute(final String containerName, final Supplier<Container> factory, final Consumer<Container> validateExisting) {
        Container container = openedContainers.get(containerName);
        if (container == null) {
            synchronized (this) {
                container = openedContainers.get(containerName);
                if (container == null) {
                    container = factory.get();
                    if (container != null) {
                        openedContainers.put(containerName, container);
                    }
                    return container;
                }
            }
        }
        if (container != null && validateExisting != null) {
            validateExisting.accept(container);
        }
        return container;
    }

    /**
     * Creates a new container list
     *
     * @param media the reference to the file
     */
    public ContainerListOperations(final Media media, final ContainerListProperties containerListProperties) {
         keyBlockOperations = KeyBlockOperations.create(media.getRootBlockAllocator(),
                 RecordLengths.standardRecordLengths(media.getMediaProperties()));
         containerListHeader = new ContainerListHeader(containerListProperties);
         containerListHeader.setKeyStartIndex(keyBlockOperations.getStartIndex());
    }

    /**
     * Loads an existing container list
     *
     * @param media
     * @param containerListHeader
     */
    public ContainerListOperations(final Media media, final ContainerListHeader containerListHeader) {
        this.keyBlockOperations = KeyBlockOperations.load(media.getRootBlockAllocator(),
                containerListHeader.getKeyStartIndex(),
                RecordLengths.standardRecordLengths(media.getMediaProperties()));
        this.containerListHeader = containerListHeader;
    }

}
