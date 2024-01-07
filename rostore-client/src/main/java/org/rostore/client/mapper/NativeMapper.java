package org.rostore.client.mapper;

import org.rostore.client.ContentType;
import org.rostore.mapper.MapperProperties;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

/**
 * This one uses the native serializer of the ro-store
 */
public class NativeMapper implements Mapper {

    private final MapperProperties mapperProperties;

    public MapperProperties getMapperProperties() {
        return mapperProperties;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K,V> InputStream fromObject(final V value, final K key) {
        final ByteArrayOutputStream byos = new ByteArrayOutputStream();
        org.rostore.mapper.BinaryMapper.serialize(mapperProperties, value, value.getClass(), byos);
        byte[] data = byos.toByteArray();
        return new ByteArrayInputStream(data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K,V> V toObject(final InputStream inputStream, final Class<V> valueClass, final K key) {
        return org.rostore.mapper.BinaryMapper.deserialize(mapperProperties, valueClass, inputStream);
    }

    public NativeMapper(final MapperProperties mapperProperties) {
        this.mapperProperties = mapperProperties;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ContentType getContentType() {
        return ContentType.BINARY;
    }
}
