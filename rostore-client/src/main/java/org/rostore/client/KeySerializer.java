package org.rostore.client;

/**
 * Interface specifies the logic for key object serialization
 * @param <K> a key object type
 */
public interface KeySerializer<K> {

    /**
     * Transforms the key object to the string
     * @param key a key object
     * @return a serialized representation of the key as a string
     */
    String toString(final K key);

}
