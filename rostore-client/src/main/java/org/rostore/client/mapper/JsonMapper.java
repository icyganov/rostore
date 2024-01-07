package org.rostore.client.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.rostore.client.ClientException;
import org.rostore.client.ContentType;

import java.io.*;

/**
 * This one uses the {@link ObjectMapper} to serialize / deserialize the content of the store
 */
public class JsonMapper implements Mapper {

    private final ObjectMapper objectMapper;

    public JsonMapper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * {@inheritDoc}
     */
    public <K,V> InputStream fromObject(final V value, final K key) {
        try {
            final ByteArrayOutputStream byos = new ByteArrayOutputStream();
            objectMapper.writeValue(byos, value);
            byte[] data = byos.toByteArray();
            return new ByteArrayInputStream(data);
        } catch (IOException e) {
            throw new ClientException("Exception while serializing object with key=\"" + key + "\".", null, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    public <K,V> V toObject(final InputStream inputStream, final Class<V> valueClass, final K key) {
        try {
            return objectMapper.readValue(new InputStreamReader(inputStream), valueClass);
        } catch (final IOException e) {
            throw new ClientException("Exception while deserializing object with key=\"" + key + "\".", null, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContentType getContentType() {
        return ContentType.JSON;
    }

}
