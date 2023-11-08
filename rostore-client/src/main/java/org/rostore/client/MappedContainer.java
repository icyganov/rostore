package org.rostore.client;

import org.rostore.Utils;
import org.rostore.client.mapper.Mapper;
import org.rostore.entity.media.RecordOption;

import java.util.EnumSet;
import java.util.function.Function;

/**
 * Encapsulates access to the container that uses specific serializer / deserializer
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

    public <V> VersionedObject<K,V> update(final VersionedObject<K,V> original,
                                           final Function<V, V> updateFunction,
                                           final KeyExpirationUpdateFunction keyExpirationUpdateFunction) {

        return generalContainer.update(original,
                keyExpirationUpdateFunction,
                updateFunction,
                (v) -> mapper.fromObject(v, original.getKey()),
                (is) -> mapper.toObject(is, original.getObjectClass(), original.getKey()));
    }

    public <V> VersionedObject<K,V> update(final VersionedObject<K,V> original,
                                           final Function<V, V> updateFunction) {
        return update(original,
                updateFunction,
                null);
    }

    public <V> VersionedObject<K, V> post(final VersionedObject<K, V> versionedObject) {
        return post(versionedObject, null);
    }

    public <V> VersionedObject<K,V> post(final VersionedObject<K,V> versionedObject, EnumSet<RecordOption> options) {
        return generalContainer.post(versionedObject,
                options,
                (t) -> mapper.fromObject(versionedObject.getValue(), versionedObject.getKey()));
    }

    public boolean removeKey(final K key) {
        return removeKey(key, Utils.VERSION_UNDEFINED, EnumSet.noneOf(RecordOption.class));
    }

    public boolean removeKey(final K key, final long version) {
        return generalContainer.removeKey(key, version, EnumSet.noneOf(RecordOption.class));
    }

    public boolean removeKey(final K key, final long version, EnumSet<RecordOption> options) {
        return generalContainer.removeKey(key, version, options);
    }

    public String getName() {
        return generalContainer.getName();
    }

    public <V> VersionedObject<K,V> get(final K key, final Class<V> clazz) {
        return generalContainer.get(key,
                (is) -> mapper.toObject(is, clazz, key));
    }

}
