package org.rostore.client;

/**
 * A default key serializer. It uses {@link Object#toString()} function.
 * @param <K> a key object type
 */
public class DefaultKeySerializer<K> implements KeySerializer<K> {

    public static final KeySerializer INSTANCE = new DefaultKeySerializer<>();

    @Override
    public String toString(final K key) {
        return key.toString();
    }

    private DefaultKeySerializer() {
    }
}
