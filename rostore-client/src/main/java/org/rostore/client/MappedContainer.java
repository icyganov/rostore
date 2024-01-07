package org.rostore.client;

import org.rostore.Utils;
import org.rostore.client.mapper.Mapper;
import org.rostore.entity.media.ContainerMeta;
import org.rostore.entity.media.RecordOption;

import java.util.EnumSet;
import java.util.function.Function;

/**
 * Encapsulates access to the container that uses specific serializer / deserializer.
 * <p>To create mapped container use {@link RoStoreClient#getMappedContainer(String, Mapper)}</p>
 *
 * @param <K> the type of the key
 */
public class MappedContainer<K> {

    private final GeneralContainer<K> generalContainer;

    private final Mapper mapper;

    protected MappedContainer(final RoStoreClient roStoreClient,
                           final String name,
                           final Mapper mapper,
                           final KeySerializer<K> keySerializer) {

        this.mapper = mapper;
        generalContainer = roStoreClient.getGeneralContainer(name, keySerializer);
    }

    public void remove() {
        generalContainer.remove();
    }

    /**
     * Creates a new container
     * @param containerMeta container meta data
     */
    public void create(final ContainerMeta containerMeta) {
        generalContainer.create(containerMeta);
    }

    /**
     * Updates a versioned key-value pair.
     *
     * <p>This operation is especially important for versioned key-values.</p>
     * <p>If the value has been changed on the remote end, this operation will detect it and reload the object, then the
     * update function will again be applied. If the update of the key will fail again, because the remote
     * object has changed in a meantime, the get operation will be repeated. Until it is successful, or
     * a defined number of attempts has reached.</p>
     *
     * @param original the original key-value pair, obtained previously over the get or post operation
     * @param updateFunction function to update the value
     * @param keyExpirationUpdateFunction function to change the expiration TTL
     * @return an updated key-value
     * @param <V> the type of the value
     */
    public <V> VersionedObject<K,V> update(final VersionedObject<K,V> original,
                                           final Function<V, V> updateFunction,
                                           final KeyExpirationUpdateFunction keyExpirationUpdateFunction) {

        return generalContainer.update(original,
                keyExpirationUpdateFunction,
                updateFunction,
                (v) -> mapper.fromObject(v, original.getKey()),
                (is) -> mapper.toObject(is, original.getObjectClass(), original.getKey()));
    }

    /**
     * Updates a versioned key-value pair.
     *
     * <p>This operation is especially important for versioned key-values.</p>
     * <p>This is similar to {@link #update(VersionedObject, Function, KeyExpirationUpdateFunction)},
     * but lacks the keyExpirationUpdateFunction, which just re-apply the expiration properties
     * of the remote version of the object</p>
     *
     * @param original the original key-value pair, obtained previously over the get or post operation
     * @param updateFunction function to update the value
     * @return an updated key-value
     * @param <V> the type of the value
     */
    public <V> VersionedObject<K,V> update(final VersionedObject<K,V> original,
                                           final Function<V, V> updateFunction) {
        return update(original,
                updateFunction,
                null);
    }

    /**
     * Creates a new key-value pair.
     *
     * @param versionedObject is created by one of the object's static functions
     * @return the versionedObject as it comes from the remote
     * @param <V> the type of the value
     */
    public <V> VersionedObject<K, V> post(final VersionedObject<K, V> versionedObject) {
        return post(versionedObject, null);
    }

    /**
     * Creates a new key-value pair.
     *
     * <p>This variant of the post allows to set one or several {@link RecordOption} to
     * modify the default behaviour of the post operation</p>
     *
     * @param versionedObject is created by one of the object's static functions
     * @param options the options to specify the behaviour of the operation in different conditions
     * @return the versionedObject as it comes from the remote
     * @param <V> the type of the value
     */
    public <V> VersionedObject<K,V> post(final VersionedObject<K,V> versionedObject, EnumSet<RecordOption> options) {
        return generalContainer.post(versionedObject,
                options,
                (t) -> mapper.fromObject(versionedObject.getValue(), versionedObject.getKey()));
    }

    /**
     * Remove a key from the storage
     *
     * @param key the key to remove
     * @return {@code true} if object existed on the storage and has been successfully removed
     */
    public boolean removeKey(final K key) {
        return removeKey(key, Utils.VERSION_UNDEFINED, EnumSet.noneOf(RecordOption.class));
    }

    /**
     * Remove a key from the storage
     *
     * <p>The operation will respect the version of the local object and will fail if the local version
     * if outdated.</p>
     *
     * @param key the key to remove
     * @return {@code true} if object existed on the storage and has been successfully removed
     */
    public boolean removeKey(final K key, final long version) {
        return generalContainer.removeKey(key, version, EnumSet.noneOf(RecordOption.class));
    }

    /**
     * Remove a key from the storage
     *
     * <p>The operation will respect the version of the local object and will fail if the local version
     * if outdated.</p>
     * <p>A set options can be provided to modify the default behaviour of the operation.</p>
     *
     * @param key the key to remove
     * @return {@code true} if object existed on the storage and has been successfully removed
     */
    public boolean removeKey(final K key, final long version, EnumSet<RecordOption> options) {
        return generalContainer.removeKey(key, version, options);
    }

    /**
     * @return the name of the container
     */
    public String getName() {
        return generalContainer.getName();
    }

    /**
     * Retrieves the object from the storage
     *
     * @param key the key object
     * @param valueClass the class of the value object
     * @return the key-value pair with metadata like TTL, version, and other
     * @param <V> the type of the value object
     */
    public <V> VersionedObject<K,V> get(final K key, final Class<V> valueClass) {
        return generalContainer.get(key,
                (is) -> mapper.toObject(is, valueClass, key));
    }

}
