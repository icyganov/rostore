package org.rostore.v2.container.async;

import org.rostore.Utils;
import org.rostore.entity.Record;
import org.rostore.entity.RoStoreException;
import org.rostore.entity.StringKeyList;
import org.rostore.v2.container.DataWithRecord;
import org.rostore.v2.keys.KeyList;
import org.rostore.v2.container.*;
import org.rostore.mapper.BinaryMapper;
import org.rostore.v2.media.Closeable;
import org.rostore.v2.media.block.container.Status;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A representation of the rostore container.
 */
public class AsyncContainer implements Closeable {
    private final Container container;
    private final List<ContainerShardExecutor> containerShardExecutors;
    private long lastExecutorWentIdleTimestamp;
    private boolean shutdown;
    private AsyncContainers asyncContainers;

    public void closeIfIdle(final long idleMillis) {
        if (!shutdown) {
            boolean shutdownStarted = false;
            synchronized (this) {
                if (isIdle()) {
                    long howLongIdle = lastExecutorWentIdleTimestamp - System.currentTimeMillis();
                    if (howLongIdle > idleMillis) {
                        shutdown();
                        shutdownStarted = true;
                    }
                }
            }
            if (shutdownStarted) {
                waitForShutdown();
                container.close();
                asyncContainers.evict(container.getName());
            }
        }

    }

    public Container getContainer() {
        return container;
    }

    private synchronized void shutdown() {
        shutdown =  true;
        for (int i = 0; i < containerShardExecutors.size(); i++) {
            if (containerShardExecutors.get(i) != null) {
                containerShardExecutors.get(i).shutdown();
            }
        }
    }

    private void shutdownAndWait() {
        shutdown();
        waitForShutdown();
    }

    public void waitForShutdown() {
        if (!shutdown) {
            throw new AsyncContainerAccessException("Can't wait for an active async container.");
        }
        for (int i = 0; i < containerShardExecutors.size(); i++) {
            if (containerShardExecutors.get(i) != null) {
                containerShardExecutors.get(i).shutdownAndWait();
            }
        }
    }


    public synchronized boolean isIdle() {
        boolean atLeastOneIsStillRunning = false;
        for (int i = 0; i < containerShardExecutors.size(); i++) {
            if (containerShardExecutors.get(i) != null) {
                atLeastOneIsStillRunning = atLeastOneIsStillRunning || !containerShardExecutors.get(i).isIdle();
            }
        }
        return atLeastOneIsStillRunning == false;
    }

    public void notifyIdle(final ContainerShardExecutor containerShardExecutor) {
        lastExecutorWentIdleTimestamp = System.currentTimeMillis();
    }

    public AsyncContainers getAsyncContainers() {
        return asyncContainers;
    }

    public AsyncContainer(final AsyncContainers asyncContainers, final Container container) {
        this.container = container;
        this.asyncContainers = asyncContainers;
        containerShardExecutors = new ArrayList<>(container.getDescriptor().getContainerMeta().getShardNumber());
        for(int i = 0; i< container.getDescriptor().getContainerMeta().getShardNumber(); i++) {
            containerShardExecutors.add(null);
        }
        shutdown = false;
        lastExecutorWentIdleTimestamp = System.currentTimeMillis();
    }

    private static <T> T resolveFuture(final Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            throw new RoStoreException("The execution has been interrupted", e);
        } catch(final OperationExecutionException e) {
            throw new OperationExecutionRuntimeException(e);
        } catch (final ExecutionException e) {
            throw new RoStoreException("Unknown execution failure", e);
        }
    }

    /**
     * Puts a key-value pair to the container
     *
     * @param sessionId the id of the session
     * @param key the key data
     * @param valueStream the stream with the value data
     * @param record the properties of the key-value pair (ttl, options, versions)
     * @param <T> the type of the data
     */
    public <T extends InputStream> void put(final int sessionId, final byte[] key, final AsyncStream<T> valueStream, final Record record) {
        putAsync(sessionId, key, valueStream, record);
    }

    /**
     * Puts a key-value pair to container
     *
     * @param sessionId the id of the session
     * @param key the key data
     * @param value the value object, that will be deserialized by {@link BinaryMapper}
     * @param record the properties of the key-value pair (ttl, options, versions)
     * @param <T> the type of the data
     */
    public <T> void put(final int sessionId, final byte[] key, final T value, final Record record) {
        put(sessionId, key, (outputStream) -> BinaryMapper.serialize(container.getContainerListOperations().getMedia().getMediaProperties().getMapperProperties(), value, outputStream), record);
    }

    /**
     * Puts a key-value pair to container.
     *
     * <p>The version is set to 0 (non-versioned), ttl is set to 0 (eternal)</p>
     *
     * @param sessionId the id of the session
     * @param key the key data
     * @param value the value object, that will be deserialized by {@link BinaryMapper}
     * @param <T> the type of the data
     */
    public <T> Record put(final int sessionId, final String key, final T value) {
        final Record record = new Record();
        put(sessionId, key, value, record);
        return record;
    }

    /**
     * Puts a key-value pair to container.
     *
     * @param sessionId the id of the session
     * @param key the key data
     * @param value the value object, that will be deserialized by {@link BinaryMapper}
     * @param record the properties of the key-value pair (ttl, options, versions)
     * @param <T> the type of the data
     */
    public <T> void put(final int sessionId, final String key, final T value, Record record) {
        put(sessionId, key.getBytes(StandardCharsets.UTF_8), value, record);
    }

    /**
     * Puts a key-value pair to container.
     *
     * <p>The version is set to 0 (non-versioned), ttl is set to 0 (eternal)</p>
     *
     * @param sessionId the id of the session
     * @param key the key string
     * @param valueOutputStream a consumer that should provide an OutputStream with the value
     */
    public Record put(final int sessionId, final String key, Consumer<OutputStream> valueOutputStream) {
        return put(sessionId, key.getBytes(StandardCharsets.UTF_8), valueOutputStream);
    }

    /**
     * Puts a key-value pair to container.
     *
     * <p>The version is set to 0 (non-versioned), ttl is set to 0 (eternal)</p>
     *
     * @param sessionId the id of the session
     * @param key the key data
     * @param valueOutputStream a consumer that should provide an OutputStream with the value
     */
    public Record put(final int sessionId, final byte[] key, Consumer<OutputStream> valueOutputStream) {
        final Record record = new Record();
        put(sessionId, key, valueOutputStream, record);
        return record;
    }

    /**
     * Puts a key-value pair to container.
     *
     * @param sessionId the id of the session
     * @param key the key data
     * @param valueOutputStream a consumer that should provide an OutputStream with the value
     * @param record the properties of the key-value pair (ttl, options, versions)
     */
    public void put(final int sessionId, final byte[] key, Consumer<OutputStream> valueOutputStream, Record record) {
        try (final PipedOutputStream pipedOutputStream = new PipedOutputStream()) {
            final PipedInputStream pipedInputStream = new PipedInputStream(pipedOutputStream);
            final AsyncStream<PipedInputStream> asyncStream = AsyncStream.wrap(pipedInputStream);
            putAsync(sessionId, key, asyncStream, record);
            valueOutputStream.accept(pipedOutputStream);
        } catch (final IOException e) {
            throw new RoStoreException("Problem in the piped output", e);
        }
    }

    /**
     * Puts a key-value pair to container.
     *
     * <p>The version is set to 0 (non-versioned), ttl is set to 0 (eternal)</p>
     *
     * @param sessionId the id of the session
     * @param key the key data
     * @param asyncStream an async stream on the basis of InputStream to read from
     * @param <T> the subtype of the InputStream to read value
     */
    public <T extends InputStream> void putAsync(final int sessionId, final byte[] key, final AsyncStream<T> asyncStream) {
        putAsync(sessionId, key, asyncStream, new Record());
    }

    /**
     * Puts a key-value pair to container.
     *
     * @param sessionId the id of the session
     * @param key the key data
     * @param asyncStream an async stream on the basis of InputStream to read from
     * @param record the properties of the key-value pair (ttl, options, versions)
     * @param <T> the subtype of the InputStream to read value
     */
    public <T extends InputStream> void putAsync(final int sessionId, final byte[] key, final AsyncStream<T> asyncStream, final Record record) {
        final ContainerShardExecutor shardExecutor = getShardExecutorByKey(key);
        shardExecutor.executeValue(sessionId, OperationType.WRITE, 0, true,
            () -> {
                asyncStream.processFunction((inputStream) -> {
                    // Start copying the data
                    final long id = shardExecutor.getShard().putValue(inputStream);
                    record.id(id);
                    shardExecutor.executeKey(sessionId, OperationType.WRITE, false, (ops) -> {
                        long prevId;
                        try {
                            prevId = ops.putKey(key, record);
                            asyncStream.notifyRecord(record);
                        } catch (final Exception e) {
                            // clean up of the value if the key can't be created
                            if (record.getId() != Utils.ID_UNDEFINED) {
                                // if the record has been created, it needs to be removed
                                shardExecutor.executeAutonomousValue(sessionId,
                                        OperationType.DELETE,
                                        record.getId(),
                                        false,
                                        () -> shardExecutor.getShard().removeValue(record.getId())
                                );
                            }
                            throw e;
                        }
                        // clean up of the previous value if the key has been created
                        if (prevId != Utils.ID_UNDEFINED) {
                            shardExecutor.executeAutonomousValue(sessionId, OperationType.DELETE, prevId, false, () ->
                                    shardExecutor.getShard().removeValue(prevId)
                            );
                        }
                        return record;
                    }).get();
                });
                return true;
            }
        );
    }

    /**
     * Gets a value based on key from container.
     *
     * @param sessionId the id of the session
     * @param key the key data
     * @param asyncStream an async stream on the basis of OutputStream to read the value from
     * @param <T> the subtype of the OutputStream to read value
     */
    public <T extends OutputStream> void getAsync(final int sessionId, final byte[] key, final AsyncStream<T> asyncStream) {
        ContainerShardExecutor shardExecutor = getShardExecutorByKey(key);
        shardExecutor.executeKey(sessionId, OperationType.READ, true, (ops) -> {
            // first store the data in unsync manner
            try {
                final Record record = ops.getKey(key);
                if (record != null) {
                    asyncStream.notifyRecord(record);
                    if (record.getId() != Utils.ID_UNDEFINED) {
                        shardExecutor.executeAutonomousValue(sessionId,
                                OperationType.READ,
                                record.getId(),
                                false,
                                () -> asyncStream.processFunction((outputStream) ->
                                    shardExecutor.getShard().getValue(record, outputStream)
                                ));
                    } else {
                        asyncStream.empty();
                    }
                } else {
                    asyncStream.cancel(true);
                }
                return record;
            } catch (final Exception e) {
                asyncStream.fail(e);
                throw e;
            }
        });
    }

    /**
     * Gets a value based on key from container.
     *
     * @param sessionId the id of the session
     * @param key the key data
     * @param clazz an expected class of the value object
     * @param <T> the value class
     * @return the value transformed by {@link BinaryMapper}
     */
    public <T> DataWithRecord<T> get(final int sessionId, final byte[] key, final Class<T> clazz) {
        return get(sessionId, key, (inputStream) -> BinaryMapper.deserialize(container.getContainerListOperations().getMedia().getMediaProperties().getMapperProperties(), clazz, inputStream));
    }

    /**
     * Gets a value based on key from container.
     *
     * @param sessionId the id of the session
     * @param key the key string
     * @param clazz an expected class of the value object
     * @return the value transformed by {@link BinaryMapper}
     * @param <T> the value class
     */
    public <T> DataWithRecord<T> get(final int sessionId, final String key, final Class<T> clazz) {
        return get(sessionId, key.getBytes(StandardCharsets.UTF_8), clazz);
    }

    /**
     * Gets a value based on key from container.
     *
     * @param sessionId the id of the session
     * @param key the key data
     * @param valueDeserializer a function that transforms the value input stream to the java object
     * @param <T> the value class
     * @return the value transformed by the supplied deserializer
     */
    public <T> DataWithRecord<T> get(final int sessionId, final byte[] key, final Function<InputStream,T> valueDeserializer) {
        final T data;
        try (final PipedOutputStream pos = new PipedOutputStream()) {
            final PipedInputStream pipedInputStream = new PipedInputStream(pos);
            Record[] storedRecord = new Record[1];
            final AsyncStream<PipedOutputStream> pipedOutputStreamAsyncStream = AsyncStream.wrapBlocking(pos, new AsyncListener() {
                @Override
                public void record(Record record) {
                    storedRecord[0] = record;
                }
                @Override
                public void error(Exception e) {
                }
                @Override
                public void status(AsyncStatus asyncStatus) {
                }
            });
            getAsync(sessionId, key, pipedOutputStreamAsyncStream);
            pipedOutputStreamAsyncStream.get();
            if (storedRecord[0] == null) {
                return null;
            }
            data = valueDeserializer.apply(pipedInputStream);
            return new DataWithRecord(storedRecord[0], data);
        } catch (final IOException ioException) {
            throw new RoStoreException("Can't create a piped outputStream");
        }
    }

    /**
     * List the keys in the container
     *
     * @param sessionId the session id
     * @param startWithKey prefix of the keys to return
     * @param continuationKey the key to start with (used for pagination)
     * @param maxNumber the maximum number of entries to return
     * @param maxSize the maximum size of the returned list
     * @return a list of keys
     */
    public KeyList list(final int sessionId, final byte[] startWithKey, final byte[] continuationKey, int maxNumber, int maxSize) {
        return resolveFuture(listAsync(sessionId, startWithKey, continuationKey, maxNumber, maxSize));
    }

    /**
     * List the keys in the container
     *
     * @param sessionId the session id
     * @param startWithKey prefix of the keys to return
     * @param continuationKey the key to start with (used for pagination)
     * @param maxNumber the maximum number of entries to return
     * @param maxSize the maximum size of the returned list
     * @return a list of keys
     */
    public StringKeyList list(final int sessionId, final String startWithKey, final String continuationKey, int maxNumber, int maxSize) {
        return new StringKeyList(resolveFuture(listAsync(sessionId,
                startWithKey != null ? startWithKey.getBytes(StandardCharsets.UTF_8) : null,
                continuationKey != null ? continuationKey.getBytes(StandardCharsets.UTF_8) : null,
                maxNumber,
                maxSize)));
    }

    /**
     * List the keys in the container
     *
     * @param sessionId the session id
     * @param startWithKey prefix of the keys to return
     * @param continuationKey the key to start with (used for pagination)
     * @param maxNumber the maximum number of entries to return
     * @param maxSize the maximum size of the returned list
     * @return a future to return the list of keys
     */
    public Future<KeyList> listAsync(final int sessionId, final byte[] startWithKey, final byte[] continuationKey, int maxNumber, int maxSize) {
        return asyncContainers.getExecutorService().submit(() -> {
             KeyList result = new KeyList();
             int shardIndex = continuationKey == null ? 0 : getShardIndexByKey(continuationKey);
             byte[] iterationKey = continuationKey;
             int[] iterationParams = { maxNumber, maxSize };
             while(true) {
                 final byte[] finalIterationKey = iterationKey;
                 final ContainerShardExecutor shardExecutor = getShardExecutorByIndex(shardIndex);
                 final KeyList iteration = shardExecutor.executeKey(sessionId,
                         OperationType.READ,
                         true,
                         (ops) -> ops.listKeys(startWithKey, finalIterationKey, iterationParams[0], iterationParams[1])).
                         get();
                 result.getKeys().addAll(iteration.getKeys());
                 result.setSize(result.getSize() + iteration.getSize());
                 if (iteration.isMore()) {
                     result.setMore(true);
                     return result;
                 } else {
                     iterationParams[0] -= iteration.getKeys().size();
                     iterationParams[1] -= iteration.getSize();
                     if (iterationParams[0] <= 0 || iterationParams[1] <= 0) {
                         result.setMore(true);
                         return result;
                     }
                     shardIndex ++;
                     if (shardIndex >= getContainer().getDescriptor().getContainerMeta().getShardNumber()) {
                         // no more shards
                         return result;
                     }
                     iterationKey = null;
                 }
             }
        });
    }

    /**
     * Delete a key if one exists
     *
     * @param sessionId the session id
     * @param key the key binary
     * @param record metadata, mainly for options and version
     * @return {@code true} if deletion has been successfully executed
     */
    public boolean remove(final int sessionId, final byte[] key, Record record) {
        return resolveFuture(removeAsync(sessionId, key, record));
    }

    /**
     * Delete a key if one exists
     *
     * @param sessionId the session id
     * @param key the key binary
     * @return {@code true} if deletion has been successfully executed
     */
    public boolean remove(final int sessionId, final String key) {
        return remove(sessionId, key, new Record());
    }

    /**
     * Delete a key if one exists
     *
     * @param sessionId the session id
     * @param key the key string
     * @return {@code true} if deletion has been successfully executed
     */
    public boolean remove(final int sessionId, final String key, Record record) {
        return remove(sessionId, key.getBytes(StandardCharsets.UTF_8), record);
    }

    /**
     * Delete a key if one exists
     *
     * @param sessionId the session id
     * @param key the key's binary
     * @return a future for boolean, {@code true} if deletion has been successfully executed
     */
    public Future<Boolean> removeAsync(final int sessionId, final byte[] key, final Record record) {
        final ContainerShardExecutor shardExecutor = getShardExecutorByKey(key);
        return shardExecutor.executeKey(sessionId, OperationType.DELETE, true, (ops) -> {
            final boolean result = ops.removeKey(key, record);
            if (record.getId() == Utils.ID_UNDEFINED) {
                return result;
            } else {
                // deletion of the value happens in parallel
                shardExecutor.executeAutonomousValue(sessionId,
                        OperationType.DELETE,
                        record.getId(),
                        false,
                        () -> shardExecutor.getShard().removeValue(record.getId())
                    );
                return result;
            }
        });
    }

    private int getShardIndexByKey(final byte[] key) {
        int hashcode = computeHashCode(key);
        int shardIndex = (hashcode * container.getDescriptor().getContainerMeta().getShardNumber()) >> 8;
        return shardIndex;
    }

    public ContainerShardExecutor getShardExecutorByKey(final byte[] key) {
        final int shardIndex = getShardIndexByKey(key);
        return getShardExecutorByIndex(shardIndex);
    }

    public synchronized ContainerShardExecutor getShardExecutorByIndex(final int shardIndex) {
        if (shutdown) {
            throw new AsyncContainerAccessException("Container is in shutdown mode.");
        }
        ContainerShardExecutor containerShardExecutor = containerShardExecutors.get(shardIndex);
        if (containerShardExecutor == null) {
            containerShardExecutor = new ContainerShardExecutor(this, container.getShard(shardIndex));
            containerShardExecutors.set(shardIndex, containerShardExecutor);
        }
        return containerShardExecutor;
    }

    private int computeHashCode(final byte[] key) {
        int every = key.length / 10;
        if (every < 1) {
            every = 1;
        }
        int sum = 0;
        for(int i=0; i<key.length; i+=every) {
            sum += key[i];
        }
        return sum & 0xff;
    }

    @Override
    public void close() {
        shutdownAndWait();
        container.close();
        asyncContainers.evict(container.getName());
    }

    public void remove() {
        shutdownAndWait();
        container.getContainerListOperations().remove(container.getName());
        asyncContainers.evict(container.getName());
    }

    @Override
    public Status getStatus() {
        return container.getStatus();
    }
}
