package org.rostore.client.mapper;

import org.rostore.client.ContentType;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

/**
 * This one can only work with the keys of type {@link String}.
 * If any other type is provided an exception is thrown.
 */
public class StringMapper implements Mapper {

    /**
     * {@inheritDoc}
     */
    @Override
    public <K,V> InputStream fromObject(final V value, final K key) {
        if (value instanceof String) {
            String str = (String) value;
            return new ByteArrayInputStream(str.getBytes(StandardCharsets.UTF_8));
        }
        throw new UnsupportedOperationException("Key should be of type String");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K,V> V toObject(final InputStream inputStream, final Class<V> valueClass, final K key) {
        final String ret = new BufferedReader(new InputStreamReader(inputStream))
                .lines().collect(Collectors.joining("\n"));
        if (valueClass.isInstance(ret)) {
            return (V)ret;
        }
        throw new UnsupportedOperationException("Key should be of type String");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContentType getContentType() {
        return ContentType.BINARY;
    }
}
