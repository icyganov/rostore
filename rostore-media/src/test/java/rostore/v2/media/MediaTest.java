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

public class MediaTest {

    @Test
    public void openCloseOpen() {
        File file = TestFile.createNewFile("media-1.blck");
        MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
        mediaPropertiesBuilder.setMaxTotalSize(256*10);
        mediaPropertiesBuilder.setBlockSize(256);
        Media media = Media.create(file, MediaProperties.from(mediaPropertiesBuilder));
        MemoryAllocation memoryAllocation = media.getMemoryManagement();
        Assertions.assertEquals(5, memoryAllocation.getLockedFreeSize() / 256);
        Assertions.assertEquals(5, memoryAllocation.getPayloadSize() / 256);
        Assertions.assertEquals(10, memoryAllocation.getTotalLockedSize() / 256);

        CatalogBlockIndices ids1 = media.getBlockAllocator().allocate(BlockType.CATALOG,4);

        try {
            CatalogBlockIndices ids2 = media.getBlockAllocator().allocate(BlockType.CATALOG,1);
            Assertions.fail("Should fail, as number of blocks is depleted");
        } catch (RoStoreException roStoreException) {
        }

        Assertions.assertEquals(1, memoryAllocation.getLockedFreeSize() / 256);
        Assertions.assertEquals(9, memoryAllocation.getPayloadSize() / 256);
        Assertions.assertEquals(10, memoryAllocation.getTotalLockedSize() / 256);

        media.getBlockAllocator().free(ids1);

        Assertions.assertEquals(5, memoryAllocation.getLockedFreeSize() / 256);
        Assertions.assertEquals(5, memoryAllocation.getPayloadSize() / 256);
        Assertions.assertEquals(10, memoryAllocation.getTotalLockedSize() / 256);

        media.close();

        Media media1 = Media.open(file);
        MemoryAllocation memoryAllocation1 = media1.getMemoryManagement();
        Assertions.assertEquals(0, memoryAllocation1.getLockedFreeSize() / 256);
        Assertions.assertEquals(10, memoryAllocation1.getPayloadSize() / 256);
        Assertions.assertEquals(10, memoryAllocation1.getTotalLockedSize() / 256);
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
        Assertions.assertEquals(86, memoryAllocation.getLockedFreeSize() / 64);
        Assertions.assertEquals(14, memoryAllocation.getPayloadSize() / 64);
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
        Assertions.assertEquals(995, memoryAllocation.getLockedFreeSize() / 64);
        Assertions.assertEquals(5, memoryAllocation.getPayloadSize() / 64);
        Assertions.assertEquals(1000, memoryAllocation.getTotalLockedSize() / 64);

        BlockAllocator blockAllocator = media.getBlockAllocator();

        CatalogBlockIndices total = new CatalogBlockIndices();

        int[] HOW_MANY = new int[] {10,4,1,15,5,6,2,9,3,11};

        int sum = 0;

        for(int i=0; i<10; i++) {
            sum += HOW_MANY[i];
            CatalogBlockIndices indices = blockAllocator.allocate(BlockType.CATALOG,HOW_MANY[i]);
            Assertions.assertEquals(995-sum, memoryAllocation.getLockedFreeSize() / 64);
            Assertions.assertEquals(5+sum, memoryAllocation.getPayloadSize() / 64);
            total.add(indices);
        }

        Assertions.assertEquals(66, total.getLength());

        blockAllocator.free(total);

        Assertions.assertEquals(995, memoryAllocation.getLockedFreeSize() / 64);
        Assertions.assertEquals(5, memoryAllocation.getPayloadSize() / 64);
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
        Assertions.assertEquals(995, memoryAllocation.getLockedFreeSize() / 64);
        Assertions.assertEquals(5, memoryAllocation.getPayloadSize() / 64);
        Assertions.assertEquals(1000, memoryAllocation.getTotalLockedSize() / 64);

        BlockAllocator blockAllocator = media.getBlockAllocator();

        CatalogBlockIndices[] allocateds = new CatalogBlockIndices[10];
        for(int i=0; i<allocateds.length; i++) {
            allocateds[i] = new CatalogBlockIndices();
        }

        for(int u=0; u<500; u++) {

            int index = (int) Math.round(Math.random() * 9);
            CatalogBlockIndices allocated = allocateds[index];

            if (blockAllocator.getFreeBlocks() > 1) {
                long hm = blockAllocator.getFreeBlocks() - 1;
                int hmr = (int) Math.round(Math.random() * hm);
                if (hmr > 10) {
                    hmr = (int) (Math.round(10 * Math.random()));
                }
                allocated.add(blockAllocator.allocate(BlockType.CATALOG,hmr));
                System.out.println("[" + index + "] Allocated: " + allocated.getLength() + ", free: " + blockAllocator.getFreeBlocks());
            }

            long l = allocated.getLength();
            int hmr = (int) Math.round(Math.random() * l);
            if (hmr > 10) {
                hmr = (int) (Math.round(10 * Math.random()));
            }
            if (hmr != 0) {
                CatalogBlockIndices extracted = allocated.extract(hmr);
                System.out.println("[" + index + "]  Extracted: " + extracted.getLength() + ", allocated: " + allocated.getLength());
                blockAllocator.free(extracted);
            }
        }

        for(int i=0; i<allocateds.length; i++) {
            blockAllocator.free(allocateds[i]);
        }

        Assertions.assertEquals(993, memoryAllocation.getLockedFreeSize() / 64);
        Assertions.assertEquals(7, memoryAllocation.getPayloadSize() / 64);
        Assertions.assertEquals(1000, memoryAllocation.getTotalLockedSize() / 64);

    }



}
