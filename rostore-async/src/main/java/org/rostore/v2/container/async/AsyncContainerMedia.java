package org.rostore.v2.container.async;

import org.rostore.v2.container.ContainerListHeader;
import org.rostore.v2.media.Closeable;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.block.container.Status;

import java.io.File;
import java.util.concurrent.ExecutorService;

public class AsyncContainerMedia implements Closeable {

    private AsyncContainers asyncContainers;
    private Media media;

    public AsyncContainers getAsyncContainers() {
        return asyncContainers;
    }

    public void setAsyncContainers(AsyncContainers asyncContainers) {
        this.asyncContainers = asyncContainers;
    }

    public Media getMedia() {
        return media;
    }

    public void setMedia(Media media) {
        this.media = media;
    }

    public static AsyncContainerMedia create(final File file, final ExecutorService executorService, final AsyncContainerMediaProperties asyncContainerMediaProperties) {
        AsyncContainers[] asyncContainers = new AsyncContainers[1];
        Media media = Media.create(file, asyncContainerMediaProperties.getMediaProperties(), (m) -> {
            asyncContainers[0] = AsyncContainers.create(m, asyncContainerMediaProperties.getContainerListProperties(), executorService);
            return asyncContainers[0].getContainerListHeader();
        });
        return new AsyncContainerMedia(media, asyncContainers[0]);
    }

    public static AsyncContainerMedia load(final File file, final ExecutorService executorService) {
        AsyncContainers[] asyncContainers = new AsyncContainers[1];
        final Media media = Media.open(file, ContainerListHeader.class, (m, containerListHeader) -> {
            asyncContainers[0] = AsyncContainers.load(m, executorService, containerListHeader);
        });
        return new AsyncContainerMedia(media, asyncContainers[0]);
    }

    public static AsyncContainerMedia create(final File file, final AsyncContainerMediaProperties asyncContainerMediaProperties) {
        AsyncContainers[] asyncContainers = new AsyncContainers[1];
        final Media media = Media.create(file, asyncContainerMediaProperties.getMediaProperties(), (m) -> {
            asyncContainers[0] = AsyncContainers.create(m, asyncContainerMediaProperties.getContainerListProperties());
            return asyncContainers[0].getContainerListHeader();
        });
        return new AsyncContainerMedia(media, asyncContainers[0]);
    }

    public static AsyncContainerMedia load(final File file) {
        AsyncContainers[] asyncContainers = new AsyncContainers[1];
        final Media media = Media.open(file, ContainerListHeader.class, (m, containerListHeader) ->
            asyncContainers[0] = AsyncContainers.load(m, containerListHeader)
        );
        return new AsyncContainerMedia(media, asyncContainers[0]);
    }

    private AsyncContainerMedia(final Media media, final AsyncContainers asyncContainers) {
        this.media = media;
        this.asyncContainers = asyncContainers;
    }

    @Override
    public void close() {
        this.asyncContainers.close();
        this.media.close();
    }

    @Override
    public Status getStatus() {
        return media.getStatus();
    }
}
