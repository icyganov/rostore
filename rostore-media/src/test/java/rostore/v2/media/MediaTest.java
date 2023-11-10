package rostore.v2.media;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.rostore.entity.media.MediaPropertiesBuilder;
import rostore.TestFile;
import org.rostore.entity.MemoryAllocation;
import org.rostore.entity.RoStoreException;
import org.rostore.mapper.BlockIndex;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.MediaProperties;
import org.rostore.v2.media.block.BlockType;
import org.rostore.v2.media.block.allocator.BlockAllocator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class MediaTest {

    @Test
    public void openCloseOpen() {
        File file = TestFile.createNewFile("media-1.blck");
        MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
        mediaPropertiesBuilder.setMaxTotalSize(256*16);
        mediaPropertiesBuilder.setBlockSize(256);
        Media media = Media.create(file, MediaProperties.from(mediaPropertiesBuilder));
        MemoryAllocation memoryAllocation = media.getMemoryManagement();
        Assertions.assertEquals(14, memoryAllocation.getLockedFreeSize() / 256);
        Assertions.assertEquals(2, memoryAllocation.getPayloadSize() / 256);
        Assertions.assertEquals(16, memoryAllocation.getTotalLockedSize() / 256);

        CatalogBlockIndices ids1 = media.getBlockAllocator().allocate(BlockType.CATALOG,4);

        try {
            CatalogBlockIndices ids2 = media.getBlockAllocator().allocate(BlockType.CATALOG,1);
            Assertions.fail("Should fail, as number of blocks is depleted");
        } catch (RoStoreException roStoreException) {
        }

        memoryAllocation = media.getMemoryManagement();
        Assertions.assertEquals(10, memoryAllocation.getLockedFreeSize() / 256);
        Assertions.assertEquals(6, memoryAllocation.getPayloadSize() / 256);
        Assertions.assertEquals(16, memoryAllocation.getTotalLockedSize() / 256);

        media.getBlockAllocator().free(ids1);

        memoryAllocation = media.getMemoryManagement();
        Assertions.assertEquals(14, memoryAllocation.getLockedFreeSize() / 256);
        Assertions.assertEquals(2, memoryAllocation.getPayloadSize() / 256);
        Assertions.assertEquals(16, memoryAllocation.getTotalLockedSize() / 256);

        media.close();

        Media media1 = Media.open(file);
        MemoryAllocation memoryAllocation1 = media1.getMemoryManagement();
        Assertions.assertEquals(14, memoryAllocation1.getLockedFreeSize() / 256);
        Assertions.assertEquals(2, memoryAllocation1.getPayloadSize() / 256);
        Assertions.assertEquals(16, memoryAllocation1.getTotalLockedSize() / 256);
    }

    public static class CustomHeader {
        private String headerText;
        @BlockIndex
        private long index;
        private long upperIndex;
    }

    @Test
    public void openCloseOpen_withCustomHeader() {
        File file = TestFile.createNewFile("media-1-ch.blck");
        MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
        mediaPropertiesBuilder.setMaxTotalSize(64*100);
        mediaPropertiesBuilder.setBlockSize(64);
        long[] startIndex = new long[1];
        String text = "My important header! It is also a very long header to reach the boundary of the block! It is also a very long header to reach the boundary of the block! It is also a very long header to reach the boundary of the block! It is also a very long header to reach the boundary of the block!";
        Media media = Media.create(file, MediaProperties.from(mediaPropertiesBuilder), (m) -> {
            try (final BlockAllocator ba = m.createSecondaryBlockAllocator("blah", 50)) {
                CustomHeader ch = new CustomHeader();
                ch.headerText = text;
                ch.index = ba.getStartIndex();
                ch.upperIndex = 50;
                startIndex[0] = ch.index;
                return ch;
            }
        });
        MemoryAllocation memoryAllocation = media.getMemoryManagement();
        Assertions.assertEquals(89, memoryAllocation.getLockedFreeSize() / 64);
        Assertions.assertEquals(11, memoryAllocation.getPayloadSize() / 64);
        Assertions.assertEquals(100, memoryAllocation.getTotalLockedSize() / 64);

        media.close();

        Media.open(file, CustomHeader.class, (m, h) -> {
            Assertions.assertEquals(text, h.headerText);
            Assertions.assertEquals(startIndex[0], h.index);
            Assertions.assertEquals(50, h.upperIndex);
        });

    }

    @Test
    public void checkAllocator() {
        File file = TestFile.createNewFile("media-2.blck");
        MediaPropertiesBuilder mediaProperties = new MediaPropertiesBuilder();
        mediaProperties.setMaxTotalSize(64*1000);
        mediaProperties.setBlockSize(64);
        Media media = Media.create(file, MediaProperties.from(mediaProperties));

        MemoryAllocation memoryAllocation = media.getMemoryManagement();
        Assertions.assertEquals(998, memoryAllocation.getLockedFreeSize() / 64);
        Assertions.assertEquals(2, memoryAllocation.getPayloadSize() / 64);
        Assertions.assertEquals(1000, memoryAllocation.getTotalLockedSize() / 64);

        BlockAllocator blockAllocator = media.getBlockAllocator();

        CatalogBlockIndices total = new CatalogBlockIndices();

        int[] HOW_MANY = new int[] {10,4,1,15,5,6,2,9,3,11};

        int sum = 0;

        for(int i=0; i<10; i++) {
            sum += HOW_MANY[i];
            CatalogBlockIndices indices = blockAllocator.allocate(BlockType.CATALOG,HOW_MANY[i]);
            memoryAllocation = media.getMemoryManagement();
            Assertions.assertEquals(998-sum, memoryAllocation.getLockedFreeSize() / 64);
            Assertions.assertEquals(2+sum, memoryAllocation.getPayloadSize() / 64);
            total.add(indices);
        }

        Assertions.assertEquals(66, total.getLength());

        blockAllocator.free(total);

        memoryAllocation = media.getMemoryManagement();
        Assertions.assertEquals(998, memoryAllocation.getLockedFreeSize() / 64);
        Assertions.assertEquals(2, memoryAllocation.getPayloadSize() / 64);
        Assertions.assertEquals(1000, memoryAllocation.getTotalLockedSize() / 64);

        media.closeExpired();

        Assertions.assertEquals(1, media.getMemoryConsumption().getBlockContainerAllocated());
        Assertions.assertEquals(4, media.getMemoryConsumption().getBlocksAllocated());
        Assertions.assertEquals(1, media.getMemoryConsumption().getBlockSequencesAllocated());
    }

    @Test
    public void checkAllocatorExhausting() {
        File file = TestFile.createNewFile("media-3.blck");
        MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
        mediaPropertiesBuilder.setMaxTotalSize(64*1000);
        mediaPropertiesBuilder.setBlockSize(64);
        Media media = Media.create(file, MediaProperties.from(mediaPropertiesBuilder));

        MemoryAllocation memoryAllocation = media.getMemoryManagement();
        Assertions.assertEquals(998, memoryAllocation.getLockedFreeSize() / 64);
        Assertions.assertEquals(2, memoryAllocation.getPayloadSize() / 64);
        Assertions.assertEquals(1000, memoryAllocation.getTotalLockedSize() / 64);

        BlockAllocator blockAllocator = media.getBlockAllocator();

        int[] data = new int [] {2, 6, 1, 3, 10, 1, 2, 6, 10, 4, 9, 0, 2, 5, 4, 1, 8, 7, 8, 9, 6, 7, 5, 3, 4, 10, 1, 7, 9, 7, 3, 1, 1, 7, 3, 4, 2, 6, 4, 8, 10, 10, 5, 0, 0, 4, 1, 3, 2, 1, 4, 1, 4, 2, 1, 1, 1, 1, 8, 2, 7, 5, 7, 7, 5, 0, 2, 8, 4, 8, 1, 2, 4, 5, 0, 8, 5, 5, 3, 1, 4, 4, 1, 7, 1, 5, 6, 3, 3, 5, 6, 6, 4, 8, 9, 3, 4, 9, 8, 5, 10, 2, 2, 8, 9, 4, 1, 5, 5, 3, 5, 6, 2, 1, 2, 2, 6, 4, 2, 3, 6, 2, 4, 2, 4, 2, 4, 8, 2, 5, 0, 5, 6, 4, 4, 1, 4, 1, 7, 9, 9, 5, 2, 2, 5, 7, 4, 3, 2, 2};
        int next = 0;

        CatalogBlockIndices[] allocateds = new CatalogBlockIndices[10];
        for(int i=0; i<allocateds.length; i++) {
            allocateds[i] = new CatalogBlockIndices();
        }

        for(int u=0; u<50; u++) {

            int index = data[next++];

            CatalogBlockIndices allocated = allocateds[index];

            if (blockAllocator.getFreeBlocks() > 1) {
                int hmr = data[next++];
                allocated.add(blockAllocator.allocate(BlockType.CATALOG,hmr));
                System.out.println("[" + index + "] Allocated: " + allocated.getLength() + ", free: " + blockAllocator.getFreeBlocks());
            }

            long l = allocated.getLength();
            int hmr = data[next++];
            if (hmr != 0) {
                CatalogBlockIndices extracted = allocated.extract(hmr);
                System.out.println("[" + index + "]  Extracted: " + extracted.getLength() + ", allocated: " + allocated.getLength());
                blockAllocator.free(extracted);
            }
        }

        for(int i=0; i<allocateds.length; i++) {
            blockAllocator.free(allocateds[i]);
        }

        memoryAllocation = media.getMemoryManagement();
        Assertions.assertEquals(998, memoryAllocation.getLockedFreeSize() / 64);
        Assertions.assertEquals(2, memoryAllocation.getPayloadSize() / 64);
        Assertions.assertEquals(1000, memoryAllocation.getTotalLockedSize() / 64);

    }



}
