package org.rostore.v2.container.async;

import org.rostore.entity.RoStoreException;
import org.rostore.entity.media.ContainerListProperties;
import org.rostore.entity.media.ContainerMeta;
import org.rostore.v2.container.*;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.RootClosableImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class AsyncContainers extends RootClosableImpl {

    private final Map<String, AsyncContainer> asyncContainers = new HashMap<>();

    private final ExecutorService executorService;

    private final boolean privateExecutorService;

    private final ContainerListOperations containerListOperations;

    private final CleanupManager cleanupManager;

    private final Media media;

    public synchronized List<String> listAllContainers() {
        return containerListOperations.listAllContainers();
    }

    public CleanupManager getCleanupManager() {
        return cleanupManager;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public static AsyncContainers load(final Media media, final ExecutorService executorService, final ContainerListHeader header) {
        return new AsyncContainers(media, executorService, header);
    }

    public static AsyncContainers create(final Media media, final ContainerListProperties containerListProperties, final ExecutorService executorService) {
        return new AsyncContainers(media, containerListProperties, executorService);
    }

    public static AsyncContainers load(final Media media, final ContainerListHeader header) {
        return new AsyncContainers(media, header);
    }

    public static AsyncContainers create(final Media media, final ContainerListProperties containerListProperties) {
        return new AsyncContainers(media, containerListProperties);
    }

    private AsyncContainers(final Media media, final ExecutorService executorService, final ContainerListHeader header) {
        this.media = media;
        this.executorService = executorService;
        this.privateExecutorService = false;
        containerListOperations = new ContainerListOperations(media, header);
        cleanupManager = createCleanupManager();
    }

    private AsyncContainers(final Media media, final ContainerListHeader header) {
        this.media = media;
        this.executorService = createPrivateExecutorService();
        this.privateExecutorService = true;
        containerListOperations = new ContainerListOperations(media, header);
        cleanupManager = createCleanupManager();
    }

    private static ExecutorService createPrivateExecutorService() {
        return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);
    }

    private AsyncContainers(final Media media, final ContainerListProperties containerListProperties, final ExecutorService executorService) {
        this.media = media;
        this.executorService = executorService;
        this.privateExecutorService = false;
        containerListOperations = new ContainerListOperations(media, containerListProperties);
        cleanupManager = createCleanupManager();
    }

    private AsyncContainers(final Media media, final ContainerListProperties containerListProperties) {
        this.media = media;
        this.executorService = createPrivateExecutorService();
        privateExecutorService = true;
        containerListOperations = new ContainerListOperations(media, containerListProperties);
        cleanupManager = createCleanupManager();
    }

    private CleanupManager createCleanupManager() {
        return new CleanupManager(executorService, media.getMediaProperties().getCloseUnusedBlocksAfterMillis()) {
            @Override
            protected void execute() {
                media.closeExpired();
            }

            @Override
            protected void finalized() {

            }
        };
    }

    public ContainerListHeader getContainerListHeader() {
        return containerListOperations.getContainerListHeader();
    }

    public AsyncContainer get(final String name) {
        return getOrExecute(name,
                () -> {
                    final Container container = containerListOperations.get(name);
                    if (container == null) {
                        return null;
                    }
                    return new AsyncContainer(this, container);
                },
                null);
    }

    public AsyncContainer create(final String name, final ContainerMeta containerMeta) {
        return getOrExecute(name,
                () -> {
                    final Container container = containerListOperations.create(name, containerMeta);
                    if (container == null) {
                        return null;
                    }
                    return new AsyncContainer(this, container);
                },
                asyncContainer -> { throw new RoStoreException("The container is already created and opened"); });
    }

    public boolean close(final String name) {
        final AsyncContainer asyncContainer;
        synchronized (this) {
            asyncContainer = asyncContainers.remove(name);
            if (asyncContainer == null) {
                return false;
            }
        }
        asyncContainer.close();
        return true;
    }

    protected synchronized boolean evict(final String name) {
        final AsyncContainer asyncContainer = asyncContainers.remove(name);
        if (asyncContainer != null) {
            asyncContainer.getContainer().getContainerListOperations().evict(name);
            return true;
        }
        return false;
    }

    public synchronized boolean remove(final String name) {
        AsyncContainer asyncContainer = asyncContainers.get(name);
        if (asyncContainer != null) {
            // async container opened
            asyncContainer.remove();
            return true;
        }
        // remove container that is not opened
        return containerListOperations.remove(name);
    }

    @Override
    public void close() {
        super.close();
        final List<AsyncContainer> all = new ArrayList<>(asyncContainers.values().size());
        all.addAll(asyncContainers.values());
        for(final AsyncContainer asyncContainer : all) {
            asyncContainer.close();
        }
        if (privateExecutorService) {
            executorService.shutdown();
            try {
                executorService.awaitTermination(5, TimeUnit.SECONDS);
            } catch (final InterruptedException e) {
                throw new RoStoreException("Can't stop async containers..");
            }
        }
    }

    private AsyncContainer getOrExecute(final String containerName, final Supplier<AsyncContainer> factory, final Consumer<AsyncContainer> validateExisting) {
        AsyncContainer container = asyncContainers.get(containerName);
        if (container == null) {
            synchronized (this) {
                checkOpened();
                container = asyncContainers.get(containerName);
                if (container == null) {
                    container = factory.get();
                    if (container != null) {
                        asyncContainers.put(containerName, container);
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
}
