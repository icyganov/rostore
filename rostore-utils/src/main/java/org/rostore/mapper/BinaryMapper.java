package org.rostore.mapper;

import org.rostore.Utils;

import java.io.*;
import java.lang.reflect.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * A class to serialize and deserialize java classes to the binary stream.
 * <p>It uses some properties of the rostore, e.g. the binary length of block indexes or block offset.</p>
 * <p>Depending on the parameters of the storage it can vary.</p>
 * <p>Use {@link BlockIndex} annotation to mark fields that store the block index,
 * and {@link BlockOffset} that represents the offset within a block.</p>
 */
public class BinaryMapper {

    private static final int MAX_BYTES_FOR_LENGTH = 4;

    private interface Serializer {
        void map(final Field field, final Object object, final OutputStream outputStream, final MapperProperties mediaProperties) throws IllegalAccessException, IOException;
    }

    private interface Deserializer {
        void map(final Field field, final Object object, final InputStream inputStream, final MapperProperties mediaProperties) throws IllegalAccessException, IOException;
    }

    private static int getLongBytes(final Field field, final MapperProperties mediaProperties) {
        int bytes = 8;
        if (field.getAnnotation(BlockIndex.class) != null) {
            bytes = mediaProperties.getBytesPerBlockIndex();
        } else {
            if (field.getAnnotation(BlockOffset.class) != null) {
                bytes = mediaProperties.getBytesPerBlockOffset();
            }
        }
        return bytes;
    }

    private static final Map<Class, Serializer> serializers = Map.of(
            long.class, (field,object,data,mp) -> BinaryMapper.write(data, field.getLong(object), getLongBytes(field, mp)),
            int.class, (field,object,data,mp) -> BinaryMapper.write(data, field.getInt(object), 4),
            short.class, (field,object,data,mp) -> BinaryMapper.write(data, field.getLong(object), 2),
            byte.class, (field,object,data,mp) -> data.write(field.getByte(object))
    );

    public static void write(final OutputStream data, final String value) throws IOException {
        if (value == null) {
            writeLength(data, -1);
        } else {
            final byte[] valueData = value.getBytes(StandardCharsets.UTF_8);
            writeLength(data, valueData.length);
            data.write(valueData);
        }
    }

    public static void writeLength(final OutputStream data, final int dataLength) throws IOException {
        if (dataLength == -1) {
            write(data, 0, 1);
        } else {
            if (dataLength <= Byte.MAX_VALUE - MAX_BYTES_FOR_LENGTH - 1) {
                int length = dataLength + MAX_BYTES_FOR_LENGTH + 1;
                write(data, length, 1);
            } else {
                int lengthOfLength = Utils.computeBytesForMaxValue(dataLength);
                write(data, lengthOfLength, 1);
                write(data, dataLength, lengthOfLength);
            }
        }
    }

    private static final Map<Class, Deserializer> deserializers = Map.of(
            long.class, (field,object,data,mp) -> field.setLong(object, BinaryMapper.read(data, getLongBytes(field, mp))),
            int.class, (field,object,data,mp) -> field.setInt(object, (int)BinaryMapper.read(data, 4)),
            short.class, (field,object,data,mp) -> field.setShort(object, (short)BinaryMapper.read(data, 2)),
            byte.class, (field,object,data,mp) -> field.setByte(object, (byte)data.read())
    );

    public static String readString(final InputStream data) throws IOException {
        int length = readLength(data);
        if (length == -1) {
            return null;
        }
        final byte[] valueData = new byte[length];
        data.read(valueData);
        return new String(valueData);
    }

    public static int readLength(final InputStream data) throws IOException {
        int lengthOfLength = (int) read(data, 1);
        if (lengthOfLength == 0) {
            return -1;
        } else {
            if (lengthOfLength > MAX_BYTES_FOR_LENGTH) {
                return lengthOfLength - MAX_BYTES_FOR_LENGTH - 1;
            } else {
                return (int) read(data, lengthOfLength);
            }
        }
    }

    public static <T> void serialize(final MapperProperties mediaProperties, final T object, final OutputStream outputStream) {
        if (object == null) {
            throw new RuntimeException("This function can only be used to serialize a non-null object");
        }
        serialize(mediaProperties, object, object.getClass(), outputStream);
    }

    public static <T> void serialize(final MapperProperties mediaProperties, final T object, final Type type, final OutputStream outputStream) {
        Class<T> clazz;
        if (type instanceof Class) {
            clazz = (Class)type;
        } else {
            clazz = (Class)((ParameterizedType)type).getRawType();
        }
        try {
            if (clazz.isEnum()) {
                final T[] enumConsts = clazz.getEnumConstants();
                int bytes = Utils.computeBytesForMaxValue(enumConsts.length);
                if (object == null) {
                    write(outputStream, 0, bytes);
                } else {
                    int i = Arrays.binarySearch(enumConsts, object);
                    write(outputStream, i+1, bytes);
                }
                return;
            }
            if (clazz.isAssignableFrom(String.class)) {
                write(outputStream, (String) object);
                return;
            }
            if (Collection.class.isAssignableFrom(clazz)) {
                Collection l = (Collection)object;
                if (l == null) {
                    writeLength(outputStream, -1);
                } else {
                    final Type argType = ((ParameterizedType)type).getActualTypeArguments()[0];
                    writeLength(outputStream, l.size());
                    for (final Object o : l) {
                        serialize(mediaProperties, o, argType, outputStream);
                    }
                }
                return;
            }
            if (Map.class.isAssignableFrom(clazz)) {
                Map map = (Map)object;
                if (map == null) {
                    writeLength(outputStream, -1);
                } else {
                    final Type[] args = ((ParameterizedType)type).getActualTypeArguments();
                    final Type argTypeLeft = args[0];
                    final Type argTypeRight = args[1];
                    writeLength(outputStream, map.size());
                    final Set<Map.Entry> entries = map.entrySet();
                    for (final Map.Entry o : entries) {
                        serialize(mediaProperties, o.getKey(), argTypeLeft, outputStream);
                        serialize(mediaProperties, o.getValue(), argTypeRight, outputStream);
                    }
                }
                return;
            }
            if (object == null) {
                write(outputStream, 0, 1);
                return;
            }
            write(outputStream, 1, 1);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        for (final Field field : object.getClass().getDeclaredFields()) {
            if (isIgnore(field)) {
                continue;
            }
            try {
                field.setAccessible(true);
                final Serializer serializer = serializers.get(field.getType());
                if (serializer != null) {
                    serializer.map(field, object, outputStream, mediaProperties);
                } else {
                   final Object nextObject = field.get(object);
                   serialize(mediaProperties, nextObject, field.getGenericType(), outputStream);
                }
            } catch (final Exception runtimeException) {
                throw new IllegalStateException("Field " + field.getName() + " of type " + field.getType() + " can't be serialized", runtimeException);
            }
        }
    }

    public static <T> T deserialize(final MapperProperties mediaProperties, final Type type, final InputStream inputStream) {
        return deserialize(mediaProperties, type, inputStream, 0);
    }

    /**
     *
     * @param mapperProperties
     * @param type
     * @param inputStream
     * @param limit it will stop after deserializing that many fields
     * @return
     * @param <T>
     */
    public static <T> T deserialize(final MapperProperties mapperProperties, final Type type, final InputStream inputStream, final int limit) {
        try {
            Class<T> clazz;
            if (type instanceof Class) {
                clazz = (Class)type;
            } else {
                clazz = (Class)((ParameterizedType)type).getRawType();
            }
            if (clazz.isEnum()) {
                final T[] enumConsts = clazz.getEnumConstants();
                int bytes = Utils.computeBytesForMaxValue(enumConsts.length);
                int ordinal = (int)read(inputStream, bytes);
                if (ordinal == 0) {
                    return null;
                } else {
                    return enumConsts[ordinal-1];
                }
            }
            if (String.class.equals(clazz)) {
                return (T) readString(inputStream);
            }
            if (Collection.class.isAssignableFrom(clazz)) {
                int length = readLength(inputStream);
                if (length == -1) {
                    return null;
                } else {
                    final Type argType = ((ParameterizedType)type).getActualTypeArguments()[0];
                    Class argClass = null;
                    if (argType instanceof Class) {
                        argClass = (Class) argType;
                    }
                    Collection l = null;
                    if (List.class.isAssignableFrom(clazz)) {
                        l = new ArrayList(length);
                    } else {
                        if (Set.class.isAssignableFrom(clazz)) {
                            if (argClass != null && argClass.isEnum()) {
                                l = EnumSet.noneOf(argClass);
                            } else {
                                l = new HashSet();
                            }
                        }
                    }
                    if (l == null) {
                        throw new RuntimeException("Can't instantiate " + clazz);
                    }
                    for(int i=0; i<length; i++) {
                        Object obj = deserialize(mapperProperties, argType, inputStream);
                        l.add(obj);
                    }
                    return (T)l;
                }
            }
            if (Map.class.isAssignableFrom(clazz)) {
                int length = readLength(inputStream);
                if (length == -1) {
                    return null;
                } else {
                    Type[] args = ((ParameterizedType)type).getActualTypeArguments();
                    final Type argTypeLeft = args[0];
                    final Type argTypeRight = args[1];
                    Class argClassLeft = null;
                    if (argTypeLeft instanceof Class) {
                        argClassLeft = (Class) argTypeLeft;
                    }
                    Map map = null;
                    if (argClassLeft != null && argClassLeft.isEnum()) {
                        map = new EnumMap(argClassLeft);
                    } else {
                        if (NavigableMap.class.isAssignableFrom(clazz)) {
                            map = new TreeMap();
                        } else {
                            map = new HashMap();
                        }
                    }
                    if (map == null) {
                        throw new RuntimeException("Can't instantiate " + clazz);
                    }
                    for(int i=0; i<length; i++) {
                        Object key = deserialize(mapperProperties, argTypeLeft, inputStream);
                        Object value = deserialize(mapperProperties, argTypeRight, inputStream);
                        map.put(key, value);
                    }
                    return (T)map;
                }
            }
            long nullValue = read(inputStream, 1);
            if (nullValue == 0) {
                return null;
            }
            int count = 0;
            final T object = clazz.getDeclaredConstructor().newInstance();
            for (final Field field : object.getClass().getDeclaredFields()) {
                if (isIgnore(field)) {
                    continue;
                }
                field.setAccessible(true);
                final Deserializer deserializer = deserializers.get(field.getType());
                if (deserializer != null) {
                    try {
                        deserializer.map(field, object, inputStream, mapperProperties);
                    } catch (Exception runtimeException) {
                        throw new IllegalStateException("Field " + field.getName() + " of type " + field.getType() + " can't be deserialized", runtimeException);
                    }
                } else {
                    final Type genericType = field.getGenericType();
                    final Object nextObject = deserialize(mapperProperties, genericType, inputStream);
                    field.set(object, nextObject);
                }
                count++;
                if (limit != 0 && count >= limit) {
                    break;
                }
            }
            return object;
        } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException | InstantiationException | IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean isIgnore(final Field field) {
        int modifiers = field.getModifiers();
        return Modifier.isStatic(modifiers);
    }

    private static void write(final OutputStream outputStream, long value, int number) throws IOException {
        for (int i = 0; i < number; i++) {
            int b = (int)(value & 0xFF);
            outputStream.write(b);
            value >>= 8;
        }
    }

    private static long read(final InputStream inputStream, int number) throws IOException {
        long result = 0;
        for (int i = 0; i < number; i++) {
            long b = (inputStream.read() & 0xFF);
            b <<=  8*i;
            result |= b;
        }
        return result;
    }
}
