package org.rostore.v2.container.async;

//import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.rostore.entity.media.ContainerListProperties;
import org.rostore.v2.media.MediaProperties;
import org.rostore.entity.media.MediaPropertiesBuilder;

public class AsyncContainerMediaProperties {

    private MediaProperties mediaProperties = new MediaProperties();

    private ContainerListProperties containerListProperties = new ContainerListProperties();

    public MediaProperties getMediaProperties() {
        return mediaProperties;
    }

    public void setMediaProperties(MediaProperties mediaProperties) {
        this.mediaProperties = mediaProperties;
    }

    public ContainerListProperties getContainerListProperties() {
        return containerListProperties;
    }

    public void setContainerListProperties(ContainerListProperties containerListProperties) {
        this.containerListProperties = containerListProperties;
    }

    public static AsyncContainerMediaProperties defaultContainerProperties(final MediaPropertiesBuilder mediaPropertiesBuilder) {
        AsyncContainerMediaProperties r = new AsyncContainerMediaProperties();
        r.setMediaProperties(MediaProperties.from(mediaPropertiesBuilder));
        return r;
    }
}
