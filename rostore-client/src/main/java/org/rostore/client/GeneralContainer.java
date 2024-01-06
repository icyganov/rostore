package org.rostore.client;

import org.rostore.Utils;
import org.rostore.entity.media.ContainerMeta;
import org.rostore.entity.media.ContainerMetaCompatibility;
import org.rostore.entity.media.RecordOption;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * Represents a simple rostore container
 *
 * @param <K> key type
 */
public class GeneralContainer<K> {

    private static final Logger logger = Logger.getLogger(GeneralContainer.class.getName());

    private final String name;

    private final KeySerializer<K> keySerializer;

    private final RoStoreClient roStoreClient;

    protected GeneralContainer(final RoStoreClient roStoreClient, final String name, final KeySerializer<K> keySerializer) {
        this.roStoreClient = roStoreClient;
        this.name = name;
        this.keySerializer = keySerializer;
    }

    /**
     * Removes container from rostore
     */
    public void remove() {
        RequestProperties requestProperties = roStoreClient.create("/admin/container/" + name);
        roStoreClient.delete(requestProperties);
    }

    /**
     * Creates a new container
     * @param containerMeta container meta data
     */
    public void create(final ContainerMeta containerMeta) {
        RequestProperties requestProperties = roStoreClient.create("/admin/container/" + name).contentType(ContentType.JSON.getMediaType());
        final InputStream inputStream = roStoreClient.getJsonMapper().fromObject(containerMeta, "create-container");
        roStoreClient.post(requestProperties, inputStream, (response) -> null);
    }

    /**
     * Returns the container's metadata
     * @return metadata
     */
    public ContainerMeta getMeta() {
        RequestProperties requestProperties = roStoreClient.create("/admin/container/" + name + "/meta").contentType(ContentType.JSON.getMediaType());
        ContainerMetaCompatibility containerMetaCompatibility = roStoreClient.get(requestProperties, (response) -> roStoreClient.getJsonMapper().toObject(RoStoreClient.getInputStream(response), ContainerMetaCompatibility.class, "get-container-meta"));
        ContainerMeta containerMeta = new ContainerMeta();
        containerMeta.setCreationTime(containerMetaCompatibility.getCreationTime());
        containerMeta.setMaxSize(containerMetaCompatibility.getMaxSize());
        containerMeta.setMaxTTL(containerMetaCompatibility.getMaxTTL());
        if (containerMetaCompatibility.getShardNumber() != 0) {
            containerMeta.setShardNumber(containerMetaCompatibility.getShardNumber());
        } else {
            containerMeta.setShardNumber(containerMetaCompatibility.getSegmentNumber());
        }
        return containerMeta;
    }

    /**
     * Lists the keys within the container.
     *
     * <p>The list of keys can be incomplete. Check {@link StringKeyList#isMore()}, it is set to {@code true} if
     * more entries are available. {@param continuationKey} in the consequent call should be the last key from
     * the previous call.</p>
     *
     * <p>Keys can come unsorted.</p>
     *
     * @param startWithKey defines the prefix for the keys
     * @param continuationKey provides the key from the previous call
     *
     * @return the partial list of the keys
     */
    public StringKeyList listKeys(final String startWithKey, final String continuationKey) {
        StringBuilder builder = new StringBuilder("/container/");
        builder.append(name).append("/keys");
        String query = "?";
        if (startWithKey != null) {
            builder.append(query).append("start-with-key=");
            builder.append(startWithKey);
            query = "&";
        }
        if (continuationKey != null) {
            builder.append(query).append("continuation-key=");
            builder.append(continuationKey);
        }
        String path = builder.toString();
        RequestProperties requestProperties = roStoreClient.create(path).contentType(ContentType.JSON.getMediaType());
        return roStoreClient.get(requestProperties, (httpResponse) ->
            roStoreClient.getJsonMapper().toObject(RoStoreClient.getInputStream(httpResponse), StringKeyList.class, "key-list")
        );
    }

    /**
     * Updates the given entity
     *
     * @param original the original entity
     * @param keyExpirationUpdateFunction function that defines the expiration of the key or null, if the update should use the expiration used in the original orbject
     * @param updateFunction function that updates the value's object
     * @param serializer transforms the value's object to the input stream
     * @param deserializer
     * @return an updated object
     * @param <V>
     */
    public <V> VersionedObject<K,V> update(final VersionedObject<K,V> original,
                                         final KeyExpirationUpdateFunction keyExpirationUpdateFunction,
                                         final Function<V, V> updateFunction,
                                         final Function<V, InputStream> serializer,
                                         final Function<InputStream, V> deserializer) {
        VersionedObject<K,V> current = original;
        VersionConflictException baseException = null;
        for(int i = 0; i< roStoreClient.getProperties().getUpdateRetries(); i++) {
            try {
                final V updatedObject = updateFunction.apply(current.getValue());
                final VersionedObject<K,V> updated = VersionedObject.create(current.getKey(),
                        updatedObject,
                        current.getVersion(),
                        keyExpirationUpdateFunction == null ? original.getUnixEOL() : keyExpirationUpdateFunction.unixEol(current));
                return post(updated, serializer);
            } catch (final VersionConflictException conflict) {
                baseException = conflict;
                logger.fine("Conflict while posting \"" + current.getKey() + "\", message: " + conflict.getMessage());
            }
            if (i != roStoreClient.getProperties().getUpdateRetries() - 1) {
                current = get(current.getKey(), deserializer);
                try {
                    Thread.sleep((long) (roStoreClient.getProperties().getUpdateTimeoutMax().toMillis() * Math.random()));
                } catch (final InterruptedException e) {
                    break;
                }
            }
        }
        throw new ClientException("The update for key \"" + original.getKey() + "\" has failed.", baseException.getRequestProperties(), baseException);
    }

    /**
     * Updates the given entity
     *
     * @param original the original entity
     * @param updateFunction function that updates the value's object
     * @param serializer transforms the value's object to the input stream
     * @param deserializer transforms input stream to the object
     * @return an updated object
     * @param <V>
     *
     * @see #update(VersionedObject, KeyExpirationUpdateFunction, Function, Function, Function)
     */
    public <V> VersionedObject<K,V> update(final VersionedObject<K,V> original,
                                           final Function<V, V> updateFunction,
                                           final Function<V, InputStream> serializer,
                                           final Function<InputStream, V> deserializer) {
        return update(original, null, updateFunction, serializer, deserializer);
    }

    public String getKeyPath(final K key) {
        try {
            final String encodedKey = URLEncoder.encode(keySerializer.toString(key), StandardCharsets.UTF_8.toString());
            return  "/container/" + name + "/key/" + encodedKey;
        } catch (final UnsupportedEncodingException e) {
            throw new ClientException("Can't encode key \"" + key + "\"", null, e);
        }
    }

    public Boolean removeKey(final K key, long version, EnumSet<RecordOption> options) {
        final RequestProperties rp = roStoreClient.create(getKeyPath(key));
        if (version != Utils.VERSION_UNDEFINED) {
            rp.version(version);
        }
        rp.options(options);
        return roStoreClient.delete(rp);
    }

    public VersionedObject<K,InputStream> post(final VersionedObject<K,InputStream> versionedObject,
                                             final EnumSet<RecordOption> options) {
        return post(versionedObject, options, (is) -> is);
    }

    public VersionedObject<K,InputStream> post(final VersionedObject<K,InputStream> versionedObject) {
        return post(versionedObject, EnumSet.noneOf(RecordOption.class));
    }

    public <V> VersionedObject<K,V> post(final VersionedObject<K,V> versionedObject,
                                       final Function<V, InputStream> serializer) {
        return post(versionedObject, EnumSet.noneOf(RecordOption.class), serializer);
    }

    public <V> VersionedObject<K,V> post(final VersionedObject<K,V> versionedObject,
                                       final EnumSet<RecordOption> options,
                                       final Function<V, InputStream> serializer) {

        final RequestProperties rp = roStoreClient.create(getKeyPath(versionedObject.getKey())).
                version(versionedObject.getVersion()).
                unixEol(versionedObject.getUnixEOL()).
                options(options).
                contentType(ContentType.BINARY.getMediaType());

        return roStoreClient.post(rp, serializer.apply(versionedObject.getValue()), (response) -> {
            final Long version = RoStoreClient.getVersionHeader(response);
            final Long unixEOL = roStoreClient.getEOLHeader(response);
            return VersionedObject.create(versionedObject.getKey(), versionedObject.getValue(), version, unixEOL);
        });
    }

    public <V> VersionedObject<K,V> get(final K key,
                                      Function<InputStream,V> deserializer) {
        return get(key, EnumSet.noneOf(RecordOption.class), deserializer);
    }

    public <V> VersionedObject<K,V> get(final K key,
                                      final EnumSet<RecordOption> options,
                                      final Function<InputStream,V> deserializer) {
        return getWrapped(key, options, (versionedInputStream) -> {
            V v = deserializer.apply(versionedInputStream.getValue());
            return VersionedObject.createDeserialized(versionedInputStream, v);
        });
    }

    public <V> VersionedObject<K,V> getWrapped(final K key,
                                        final EnumSet<RecordOption> options,
                                        final Function<VersionedObject<K,InputStream>, VersionedObject<K,V>> transformation) {
        final RequestProperties rp = roStoreClient.create(getKeyPath(key));
        rp.contentType(ContentType.BINARY.getMediaType());
        rp.options(options);
        return roStoreClient.get(rp, (response) -> {
            final Long version = RoStoreClient.getVersionHeader(response);
            final Long unixEOL = roStoreClient.getEOLHeader(response);
            final VersionedObject<K,InputStream> versionedObject = VersionedObject.create(key, RoStoreClient.getInputStream(response), version, unixEOL);
            return transformation.apply(versionedObject);
        });
    }

    public String getName() {
        return name;
    }

}

