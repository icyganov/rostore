package org.rostore.service;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import io.vertx.core.eventbus.EventBus;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.jboss.logging.Logger;
import org.rostore.entity.RoStoreException;
import org.rostore.v2.container.async.AsyncContainerMedia;
import org.rostore.v2.container.async.AsyncContainerMediaProperties;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import javax.inject.Inject;
import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

@Startup
@ApplicationScoped
public class RoStoreAccessor {

    private final static Logger logger = Logger.getLogger(RoStoreAccessor.class.getName());
    /**
     * Close container if it hasn't been used for 1h
     */

    @Inject
    private ManagedExecutor executorService;

    @Inject @ConfigProperty(name="storeFile")
    private String storeFileName;

    @Inject @ConfigProperty(name="closeContainersOlderThan")
    private Duration closeContainersOlderThan;

    @Inject
    EventBus bus;

    private AtomicLong cleanIterations = new AtomicLong(System.currentTimeMillis()/1000);

    private AsyncContainerMedia asyncContainerMedia;

    private RoStoreState state = RoStoreState.STARTING;

    public RoStoreState getState() {
        return state;
    }

    public AsyncContainerMedia getAsyncContainerMedia() {
        return asyncContainerMedia;
    }

    public RoStoreAccessor() {

    }

    @PostConstruct
    public void open() {
        setState(RoStoreState.INITIALIZED);
        logger.infof("Open store file %s", storeFileName);
        try {
            final File storeFile = new File(storeFileName);
            if (!storeFile.exists()) {
                throw new RoStoreException("The store file " + storeFileName + " does not exist.");
            }
            asyncContainerMedia = AsyncContainerMedia.load(storeFile, executorService);
            setState(RoStoreState.OPENED);
        } catch(final Exception e) {
            logger.error("Can't open a store file", e);
            // ignore
        }
    }

    private void setState(final RoStoreState state) {
        if (state != this.state) {
            this.state = state;
            logger.infof("The ro-store state is changing to %s", state);
            bus.send("state-change", this.state);
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        setState(RoStoreState.SHUTTING_DOWN);
        logger.infof("Closing store file %s", storeFileName);
        if (asyncContainerMedia == null) {
            logger.info("The ro-store container file has not been initialized.");
        } else {
            asyncContainerMedia.close();
        }
        logger.info("The ro-store has been successfully closed.");
    }

    public void create(final AsyncContainerMediaProperties mediaProperties) {
        final File storeFile = new File(storeFileName);
        if (asyncContainerMedia != null) {
            throw new RoStoreException("The storage has already been opened.");
        }
        if (storeFile.exists()) {
            throw new RoStoreException("The file " + storeFileName + " already exists.");
        }
        logger.debugf("Creating a store at the location '%s'", storeFileName);
        asyncContainerMedia = AsyncContainerMedia.create(storeFile, executorService, mediaProperties);
        logger.infof("A new store at the location '%s' gas just been created.", storeFileName);
        setState(RoStoreState.OPENED);
    }

    public List<String> listAllContainers() {
        return asyncContainerMedia.getAsyncContainers().listAllContainers();
    }

    @Scheduled(every = "{checkContainersEvery}")
    public void checkAndCloseIfNeeded() {
        /*asyncContainerMedia.getAsyncContainers().ping();*/
    }

}
