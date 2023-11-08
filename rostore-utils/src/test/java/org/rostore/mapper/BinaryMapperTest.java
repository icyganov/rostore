package org.rostore.mapper;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

public class BinaryMapperTest {

    public static class Level2 {
        @BlockIndex
        private long blockIndex;

        @BlockOffset
        private long blockOffset;

        public void set(int i) {
            blockIndex = 100 + i;
            blockOffset = 101 + i*2;
        }
    }

    public static class Obj {

        private byte b;
        private int i;
        private long l;

        private String s;

        @BlockIndex
        private long blockIndex;

        @BlockOffset
        private long blockOffset;

        private Level2 level2;

        private List<Level2> list;

        public void set() {
            b = Byte.MAX_VALUE;
            i = Integer.MAX_VALUE;
            l = Long.MAX_VALUE;
            s = "TEST-123";
            blockIndex = 10;
            blockOffset = 11;
            level2 = new Level2();
            level2.set(0);
            list = new ArrayList<>();
            for(int i=0; i<10; i++) {
                Level2 l = new Level2();
                l.set(i);
                list.add(l);
            }
        }
    }

    @Test
    public void test() {
        MapperProperties mediaProperties = new MapperProperties();
        mediaProperties.setBytesPerBlockIndex(1);
        mediaProperties.setBytesPerBlockOffset(2);
        Obj o = new Obj();
        o.set();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryMapper.serialize(mediaProperties, o, outputStream);
        byte[] d = outputStream.toByteArray();

        //Assert.assertEquals(1+4+8+1+8+1+2+11*3+1, d.length);

        Obj r = BinaryMapper.deserialize(mediaProperties, Obj.class, new ByteArrayInputStream(d));
        Assertions.assertEquals(Byte.MAX_VALUE, r.b);
        Assertions.assertEquals(Integer.MAX_VALUE, r.i);
        Assertions.assertEquals(Long.MAX_VALUE, r.l);
        Assertions.assertEquals(10, r.blockIndex);
        Assertions.assertEquals(11, r.blockOffset);

        Assertions.assertEquals(100, r.level2.blockIndex);
        Assertions.assertEquals(101, r.level2.blockOffset);
        Assertions.assertEquals(10, r.list.size());

        for(int i=0; i<10; i++) {
            Assertions.assertEquals(100+i, r.list.get(i).blockIndex);
            Assertions.assertEquals(101+i*2, r.list.get(i).blockOffset);
        }
    }

    @Test
    public void stringTest() {
        MapperProperties mediaProperties = new MapperProperties();
        mediaProperties.setBytesPerBlockIndex(1);
        mediaProperties.setBytesPerBlockOffset(2);
        Obj o = new Obj();
        for(int j=0; j<Short.MAX_VALUE + 89; j++) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < j; i++) {
                sb.append((i % 2) == 0 ? "a" : "b");
            }
            String s = sb.toString();
            o.s = s;
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            BinaryMapper.serialize(mediaProperties, o, outputStream);
            byte[] d = outputStream.toByteArray();
            Obj des = BinaryMapper.deserialize(mediaProperties, Obj.class, new ByteArrayInputStream(d));
            Assertions.assertEquals(s, des.s);
        }

    }

    enum e {
        one,two,three;
    }

    static class SetMapTest {
        Set<e> eSet = new HashSet<>();
        Map<e,Set<e>> sMap = new HashMap();
    }

    @Test
    public void mapSetTest() {
        MapperProperties mediaProperties = new MapperProperties();
        mediaProperties.setBytesPerBlockIndex(1);
        mediaProperties.setBytesPerBlockOffset(2);

        SetMapTest test = new SetMapTest();
        test.eSet.add(e.one);
        test.sMap.put(e.one,EnumSet.of(e.two));
        test.sMap.put(e.two,EnumSet.of(e.two, e.three));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        BinaryMapper.serialize(mediaProperties, test, outputStream);
        byte[] d = outputStream.toByteArray();
        SetMapTest des = BinaryMapper.deserialize(mediaProperties, SetMapTest.class, new ByteArrayInputStream(d));
        Assertions.assertEquals(false, des.eSet.isEmpty());
        Assertions.assertEquals(false, des.sMap.isEmpty());

        Assertions.assertArrayEquals(test.eSet.toArray(), des.eSet.toArray());
        Assertions.assertArrayEquals(test.eSet.toArray(), des.eSet.toArray());

        Assertions.assertEquals(2, des.sMap.size());
        Assertions.assertArrayEquals(sort(test.sMap.keySet()), sort(des.sMap.keySet()));
        Assertions.assertArrayEquals(sort(test.sMap.get(e.one)), sort(des.sMap.get(e.one)));
        Assertions.assertArrayEquals(sort(test.sMap.get(e.two)), sort(des.sMap.get(e.two)));
    }

    private static e[] sort(Set<e> arr) {
        e[] arr2 = arr.toArray(new e[arr.size()]);
        Arrays.sort(arr2, Comparator.comparingInt(Enum::ordinal));
        return arr2;
    }
}


