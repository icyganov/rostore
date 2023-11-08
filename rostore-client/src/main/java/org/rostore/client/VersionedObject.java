package org.rostore.client;

import org.rostore.Utils;

/**
 * Versioned object, containing the object itself, and metadata: ttl information and version
 *
 * @param <K> key object type
 * @param <V> value object type
 */
public class VersionedObject<K,V> {

    private final Long version;
    private final long unixEol;
    private final K key;
    private final V value;

    /**
     * Creates an eternal versioned object
     * @param key the key object
     * @param object the value object
     * @param version the version of the entry
     * @return entity
     * @param <K> type of the key
     * @param <V> type of the value
     */
    public static <K,V> VersionedObject<K,V> createEternal(final K key, final V object, Long version) {
        return create(key, object, version, null);
    }

    /**
     * Creates an eternal unversioned object
     *
     * @param key the key object
     * @param value the value object
     * @return entity
     * @param <K> type of the key
     * @param <V> type of the value
     */
    public static <K,V> VersionedObject<K,V> createUnversionedEternal(final K key, final V value) {
        return create(key, value, Utils.VERSION_UNDEFINED, null);
    }

    /**
     * Creates first version of eternal object
     *
     * @param key the key object
     * @param value the value object
     * @return entity
     * @param <K> type of the key
     * @param <V> type of the value
     */
    public static <K, V> VersionedObject<K, V> createNewVersionedEternal(final K key, final V value) {
        return create(key, value, 1L, null);
    }

    /**
     * Creates first version of the entity with the expiration timestamp
     *
     * @param key the key object
     * @param value the value object
     * @param unixEol the unix epoch's end of entity's life
     * @return entity
     * @param <K> type of the key
     * @param <V> type of the value
     */
    public static <K, V> VersionedObject<K, V> createNewVersioned(final K key, final V value, final Long unixEol) {
        return create(key, value, 1L, unixEol);
    }

    /**
     * Creates a versioned object (general case)
     * @param key the key object
     * @param value the value object
     * @param version the version of the entry
     * @param unixEol the unix epoch's end of entity's life
     * @return entity
     * @param <K> type of the key
     * @param <V> type of the value
     */
    public static <K,V> VersionedObject<K,V> create(final K key, final V value, Long version, final Long unixEol) {
        return new VersionedObject<>(key, value, version, unixEol);
    }

    public static <K,V1,V2> VersionedObject<K,V2> createDeserialized(final VersionedObject<K,V1> original, final V2 value) {
        return new VersionedObject<>(original.key, value, original.version, original.unixEol);
    }

    private VersionedObject(final K key, final V object, final Long version, final Long unixEol) {
        this.key = key;
        this.version = version;
        this.value = object;
        this.unixEol = unixEol == null ? Utils.EOL_FOREVER : unixEol;
    }

    /**
     *
     * @return 0 if object is eternal, otherwise millisecond counter when to expire
     */
    public long getUnixEOL() {
        return unixEol;
    }

    /**
     * Returns the key's object
     * @return the key
     */
    public K getKey() {
        return key;
    }

    /**
     * Get the Class of the object
     * @return object's class
     */
    public Class<V> getObjectClass() {
        return (Class<V>) value.getClass();
    }

    /**
     * Get the object's version.
     * <p>If unversioned returns {@link Utils#VERSION_UNDEFINED}</p>
     * @return
     */
    public Long getVersion() {
        return version == null ? Utils.VERSION_UNDEFINED : version;
    }

    /**
     * Gets is the object versioned or not
     * @return {@code true} if the object versioned, {@code false} otherwise
     */
    public boolean isVersioned() {
        if (version == null) {
            return false;
        }
        return version != Utils.VERSION_UNDEFINED;
    }

    /**
     * Get the value
     * @return the object
     */
    public V getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "VersionedObject{" +
                "version=" + version +
                ", unixEol=" + unixEol +
                ", key=" + key +
                ", value=" + value +
                '}';
    }
}
