package org.rostore.client.mapper;

import org.rostore.client.ContentType;

import java.io.InputStream;

/**
 * Provides serialization / deserialization for mapping of the underlying objects
 */
public interface Mapper {

    <K,V> InputStream fromObject(final V object, final K key);

    <K,V> V toObject(final InputStream inputStream, final Class<V> clazz, final K key);

    ContentType getContentType();

}
