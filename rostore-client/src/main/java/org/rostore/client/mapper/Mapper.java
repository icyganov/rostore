package org.rostore.client.mapper;

import org.rostore.client.ContentType;

import java.io.InputStream;

/**
 * Provides serialization / deserialization for mapping of the underlying objects
 */
public interface Mapper {

    /**
     * Serialization function
     *
     * @param value the value object
     * @param key the key object
     * @return the serialized input stream
     * @param <K> the key type
     * @param <V> the value type
     */
    <K,V> InputStream fromObject(final V value, final K key);

    /**
     * Deserialization function
     *
     * @param inputStream the input stream to deserialize
     * @param valueClass the class of the value object
     * @param key the key object
     * @return the deserialized value object
     * @param <K> the key type
     * @param <V> the value type
     */
    <K,V> V toObject(final InputStream inputStream, final Class<V> valueClass, final K key);

    /**
     * The content type of the input stream
     * @return the content type used in the header
     */
    ContentType getContentType();

}
