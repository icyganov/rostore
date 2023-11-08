package rostore.v2.media.catalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.catalog.CatalogBlockIndicesIterator;

class CatalogBlockIndicesTest {

    @Test
    void remove1() {

        CatalogBlockIndices seed = new CatalogBlockIndices();
        seed.add(1,10);
        seed.add(20,35);
        seed.add(40,50);

        Assertions.assertEquals(37, seed.getLength());

        CatalogBlockIndices second = new CatalogBlockIndices();
        second.add(15, 36);

        CatalogBlockIndices left = seed.remove(second);

        assertEquals(new long[] {15,16,17,18,19,36}, left);
        assertEquals(new long[] {1,2,3,4,5,6,7,8,9,10,40,41,42,43,44,45,46,47,48,49,50}, seed);

        second = new CatalogBlockIndices();
        second.add(51,54);
        left = seed.remove(second);

        assertEquals(new long[] {51,52,53,54}, left);
        assertEquals(new long[] {1,2,3,4,5,6,7,8,9,10,40,41,42,43,44,45,46,47,48,49,50}, seed);

        second = new CatalogBlockIndices();
        second.add(15,18);
        left = seed.remove(second);

        assertEquals(new long[] {15,16,17,18}, left);
        assertEquals(new long[] {1,2,3,4,5,6,7,8,9,10,40,41,42,43,44,45,46,47,48,49,50}, seed);

        second = new CatalogBlockIndices();
        second.add(42,45);
        left = seed.remove(second);

        assertEquals(new long[] {}, left);
        assertEquals(new long[] {1,2,3,4,5,6,7,8,9,10,40,41,46,47,48,49,50}, seed);

        second = new CatalogBlockIndices();
        second.add(41,46);
        left = seed.remove(second);

        assertEquals(new long[] {42,43,44,45}, left);
        assertEquals(new long[] {1,2,3,4,5,6,7,8,9,10,40,47,48,49,50}, seed);

        second = new CatalogBlockIndices();
        second.add(50,53);
        left = seed.remove(second);

        assertEquals(new long[] {51,52,53}, left);
        assertEquals(new long[] {1,2,3,4,5,6,7,8,9,10,40,47,48,49}, seed);

        second = new CatalogBlockIndices();
        second.add(1,2);
        left = seed.remove(second);

        assertEquals(new long[] {}, left);
        assertEquals(new long[] {3,4,5,6,7,8,9,10,40,47,48,49}, seed);

        second = new CatalogBlockIndices();
        second.add(1,3);
        second.add(5,6);
        second.add(10,13);
        left = seed.remove(second);

        assertEquals(new long[] {1,2,11,12,13}, left);
        assertEquals(new long[] {4,7,8,9,40,47,48,49}, seed);

        second = new CatalogBlockIndices();
        second.add(9,48);
        left = seed.remove(second);

        assertEquals(new long[] {10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,41,42,43,44,45,46}, left);
        assertEquals(new long[] {4,7,8,49}, seed);
    }

    private void assertEquals(final long[] expected, final CatalogBlockIndices indices) {
        Assertions.assertEquals(expected.length, indices.getLength());
        CatalogBlockIndicesIterator iterator = indices.iterator();
        int i=0;
        while (iterator.left() != 0) {
            Assertions.assertEquals(expected[i], iterator.get());
            i++;
        }
    }
}