package org.rostore.v2.container;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.rostore.TestFile;
import org.rostore.Utils;
import org.rostore.entity.Record;
import org.rostore.entity.media.ContainerMeta;
import org.rostore.v2.catalog.CatalogBlockIndices;
import org.rostore.v2.container.async.AsyncContainerMedia;
import org.rostore.v2.container.async.AsyncContainerMediaProperties;
import org.rostore.v2.container.async.AsyncStream;
import org.rostore.v2.container.async.AsyncContainer;
import org.rostore.entity.media.MediaPropertiesBuilder;
import org.rostore.v2.media.block.BlockType;
import org.rostore.v2.media.block.allocator.BlockAllocatorInternal;
import org.rostore.v2.media.block.allocator.BlockVerifierListener;
import org.rostore.v2.media.block.allocator.RootBlockAllocator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class AsyncContainerMediaTest {

    @Test
    public void test() throws ExecutionException, InterruptedException {
        MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
        mediaPropertiesBuilder.setMaxTotalSize(40*100000);
        mediaPropertiesBuilder.setBlockSize(40);
        File file = TestFile.createNewFile("media-async.blck");
        try (AsyncContainerMedia media = AsyncContainerMedia.create(file, AsyncContainerMediaProperties.defaultContainerProperties(mediaPropertiesBuilder))) {
            ContainerMeta containerMeta = new ContainerMeta();
            containerMeta.setShardNumber(10);
            try (AsyncContainer container = media.getAsyncContainers().create("central", containerMeta)){

                put(container,"key1", "value1").get();
                String value1 = getAndWait(container, "key1");
                Assertions.assertEquals("value1", value1);
                boolean removed = remove(container,"key1").get();
                Assertions.assertEquals(true, removed);
                removed = remove(container,"key1").get();
                Assertions.assertEquals(false, removed);
            }
        }
    }

    @Test
    public void testHuge() throws ExecutionException, InterruptedException {
        MediaPropertiesBuilder mediaProperties = new MediaPropertiesBuilder();
        mediaProperties.setMaxTotalSize(1000000);
        mediaProperties.setBlockSize(4096);
        mediaProperties.setCloseUnusedBlocksAfterMillis(100);
        File file = TestFile.createNewFile("media-async-8.blck");

        long usedBeforeMedia;
        long freeBeforeMedia;

        try (final AsyncContainerMedia media = AsyncContainerMedia.create(file, AsyncContainerMediaProperties.defaultContainerProperties(mediaProperties))) {
            ContainerMeta containerMeta = new ContainerMeta();
            containerMeta.setShardNumber(1);
            usedBeforeMedia = media.getMedia().getMemoryManagement().getPayloadSize();
            freeBeforeMedia = media.getMedia().getMemoryManagement().getLockedFreeSize();

            List<String> containerNames = new ArrayList<>();
            for (int j=0; j<5; j++) {
                String containerName = getRandomString(10, 15);
                containerNames.add(containerName);
                final AsyncContainer container = media.getAsyncContainers().create(containerName, containerMeta);

                long usedBeforeContainer = media.getMedia().getMemoryManagement().getPayloadSize();
                long freeBeforeContainer = media.getMedia().getMemoryManagement().getLockedFreeSize();

                List<Future<?>> futures = new ArrayList<>();

                for(int i=0; i<10;i++) {
                    String key = getRandomString(10, 15);
                    String huge = getRandomString(1500000, 1500000);
                    futures.add(put(container, key, huge));
                }
                for (int i=0; i<10; i++) {
                    try {
                        futures.get(i).get();
                        Assertions.fail("Put should fail, as the size to small");
                    } catch (Exception e) {
                    }
                }
                long usedAfterContainer = media.getMedia().getMemoryManagement().getPayloadSize();
                long freeAfterContainer = media.getMedia().getMemoryManagement().getLockedFreeSize();
                Assertions.assertEquals(freeAfterContainer,freeBeforeContainer,"Free size is wrong");
                Assertions.assertEquals(usedAfterContainer,usedBeforeContainer,"Used size is wrong");
            }

            for (String containerName : containerNames) {
                media.getAsyncContainers().get(containerName).close();
                media.getAsyncContainers().remove(containerName);
            }

            media.getMedia().getBlockAllocator().getBlockAllocatorInternal().dump();
            long usedAfterMedia = media.getMedia().getMemoryManagement().getPayloadSize();
            long freeAfterMedia = media.getMedia().getMemoryManagement().getLockedFreeSize();
            Assertions.assertEquals(freeAfterMedia,freeBeforeMedia,"Free size is wrong");
            Assertions.assertEquals(usedAfterMedia,usedBeforeMedia,"Used size is wrong");
        }

        try (final AsyncContainerMedia media = AsyncContainerMedia.load(file)) {
            long usedAfterMedia = media.getMedia().getMemoryManagement().getPayloadSize();
            long freeAfterMedia = media.getMedia().getMemoryManagement().getLockedFreeSize();
            media.getMedia().getBlockAllocator().getBlockAllocatorInternal().dump();
            Assertions.assertEquals(freeAfterMedia,freeBeforeMedia,"Free size is wrong");
            Assertions.assertEquals(usedAfterMedia,usedBeforeMedia,"Used size is wrong");
        }
    }

    @Test
    public void testAsync() throws ExecutionException, InterruptedException {
        MediaPropertiesBuilder mediaProperties = new MediaPropertiesBuilder();
        mediaProperties.setMaxTotalSize(4096L*1000000L);
        mediaProperties.setBlockSize(4096);
        mediaProperties.setCloseUnusedBlocksAfterMillis(100);
        File file = TestFile.createNewFile("media-async-2.blck");
        BlockVerifierListener listener = new BlockVerifierListener();
        // add the initial root block sequence
        CatalogBlockIndices catalogBlockIndices = new CatalogBlockIndices();
        catalogBlockIndices.add(1,4);
        listener.blocksAllocated("root", BlockType.CATALOG, catalogBlockIndices, false);
        BlockVerifierListener snapshot;

        String[] keys = {"ilbbxCtiYa","wuC3hTQrhc","5at1S5QEGp","81sVZ8ZgyK","yOH2Qqnta5"};

        try (final AsyncContainerMedia media = AsyncContainerMedia.create(file, AsyncContainerMediaProperties.defaultContainerProperties(mediaProperties))) {
            media.getMedia().getBlockAllocatorListeners().addListener(listener);
            ContainerMeta containerMeta = new ContainerMeta();
            containerMeta.setShardNumber(3);
            long usedBefore = media.getMedia().getMemoryManagement().getPayloadSize();
            long freeBefore = media.getMedia().getMemoryManagement().getLockedFreeSize();
            snapshot = listener.snapshot();
            try (final AsyncContainer container = media.getAsyncContainers().create("central", containerMeta)) {
                for(int j=0; j<10; j++) {
                    System.out.println("--> " + j);
                    Map<String, String> created = new HashMap<>();
                    List<Future<?>> writeFutures = new ArrayList<>();
                    List<Future<?>> readFutures = new ArrayList<>();

                    for (int i = 0; i < 10; i++) {
                        String key;
                        key = getRandomString(10,15);
                        System.out.println(key);
                        if (!created.containsKey(key)) {
                            created.put(key, getRandomString(15000000, 15000000));
                        }
                    }
                    System.out.println("START WRITING!");
                    long startTime = System.currentTimeMillis();
                    for (final Map.Entry<String, String> entry : created.entrySet()) {
                        writeFutures.add(put(container, entry.getKey(), entry.getValue()));
                    }
                    System.out.println("STOP SUBMITTING WRITING!");
                    for (Future<?> result : writeFutures) {
                        Object or = result.get();
                        if (or instanceof Future) {
                            ((Future) or).get();
                        }
                    }
                    System.out.println("STOP WRITING!");
                    String[] orderedValues = new String[created.entrySet().size()];
                    int i=0;
                    for (final Map.Entry<String, String> entry : created.entrySet()) {
                        readFutures.add(get(container, entry.getKey()));
                        orderedValues[i] = entry.getValue();
                        i++;
                    }
                    long writeFinished = System.currentTimeMillis();
                    System.out.println(j + " round: " + ((writeFinished-startTime)*10)/ created.size() + "/10 ms per key");
                    i=0;
                    for (Future<?> result : readFutures) {
                        if (result instanceof AsyncStream) {
                            final AsyncStream<ByteArrayOutputStream> asyncStream = (AsyncStream<ByteArrayOutputStream>)result;
                            ByteArrayOutputStream stream = asyncStream.get();
                            if (!asyncStream.isCancelled()) {
                                String s = new String(stream.toByteArray(), StandardCharsets.UTF_8);
                                Assertions.assertEquals(orderedValues[i], s);
                            }
                            i++;
                        }
                    }
                }
            }
            Thread.sleep(200);
            media.getMedia().closeExpired();
            Assertions.assertEquals(0, media.getMedia().getMemoryConsumption().getBlocksAllocated());
            media.getAsyncContainers().remove("central");
            Thread.sleep(200);
            media.getMedia().closeExpired();
            Assertions.assertEquals(media.getMedia().getMemoryManagement().getLockedFreeSize(),freeBefore,"Free size has changed");
        }
    }

    @Test
    public void testAsync2() throws ExecutionException, InterruptedException {
        MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
        mediaPropertiesBuilder.setMaxTotalSize(4096L*1000000L);
        mediaPropertiesBuilder.setBlockSize(4096);
        mediaPropertiesBuilder.setCloseUnusedBlocksAfterMillis(100);
        File file = TestFile.createNewFile("media-async-12.blck");
        BlockVerifierListener listener = new BlockVerifierListener();
        try (final AsyncContainerMedia media = AsyncContainerMedia.create(file, AsyncContainerMediaProperties.defaultContainerProperties(mediaPropertiesBuilder))) {
            //media.getMedia().getBlockAllocatorListeners().addListener(listener);
            ContainerMeta containerMeta = new ContainerMeta();
            containerMeta.setShardNumber(10);
            long usedBefore = media.getMedia().getMemoryManagement().getPayloadSize();
            long freeBefore = media.getMedia().getMemoryManagement().getLockedFreeSize();

            for(int k=0; k<10; k++) {
                String name = "central-" + k;
                System.out.println("Container: " + name);
                try (final AsyncContainer container = media.getAsyncContainers().create(name, containerMeta)) {
                    for (int j = 0; j < 10; j++) {
                        Map<String, String> created = new HashMap<>();
                        List<Future<?>> writeFutures = new ArrayList<>();
                        List<Future<?>> readFutures = new ArrayList<>();

                        for (int i = 0; i < 10; i++) {
                            String key = getRandomString(10, 50);
                            if (!created.containsKey(key)) {
                                created.put(key, getRandomString(100, 1500));
                            }
                        }
                        long startTime = System.currentTimeMillis();
                        for (final Map.Entry<String, String> entry : created.entrySet()) {
                            writeFutures.add(put(container, entry.getKey(), entry.getValue()));
                        }
                        String[] orderedValues = new String[created.entrySet().size()];
                        int i = 0;
                        for (final Map.Entry<String, String> entry : created.entrySet()) {
                            readFutures.add(get(container, entry.getKey()));
                            orderedValues[i] = entry.getValue();
                            i++;
                        }
                        for (Future<?> result : writeFutures) {
                            Object or = result.get();
                            if (or instanceof Future) {
                                ((Future) or).get();
                            }
                        }
                        System.out.println("STOP WRITING!");
                        long writeFinished = System.currentTimeMillis();
                        System.out.println(j + " round: " + ((writeFinished - startTime) * 10) / created.size() + "/10 ms per key");
                        i = 0;
                        for (Future<?> result : readFutures) {
                            if (result instanceof AsyncStream) {
                                final AsyncStream<ByteArrayOutputStream> asyncStream = (AsyncStream<ByteArrayOutputStream>) result;
                                if (!asyncStream.isCancelled()) {
                                    String s = new String(asyncStream.get().toByteArray(), StandardCharsets.UTF_8);
                                    Assertions.assertEquals(orderedValues[i], s);
                                }
                                i++;
                            }
                        }
                    }
                }
            }
            for(int k=0; k<10; k++) {
                String name = "central-" + k;
                System.out.println("Container: " + name);
                media.getAsyncContainers().remove(name);
            }
            Thread.sleep(200);
            media.getMedia().closeExpired();
            Assertions.assertEquals(0, media.getMedia().getMemoryConsumption().getBlocksAllocated());

            Assertions.assertEquals(freeBefore, media.getMedia().getMemoryManagement().getLockedFreeSize(), () -> "Free deviation: " + (media.getMedia().getMemoryManagement().getLockedFreeSize()-freeBefore)/4096);
            Assertions.assertEquals(usedBefore, media.getMedia().getMemoryManagement().getPayloadSize(), () -> "Used deviation: " + (media.getMedia().getMemoryManagement().getPayloadSize()-usedBefore)/4096);
        }
    }

    @Test
    public void testSameKeyAsync() throws ExecutionException, InterruptedException {
        MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
        mediaPropertiesBuilder.setMaxTotalSize(4096L*1000000L);
        mediaPropertiesBuilder.setBlockSize(4096);
        mediaPropertiesBuilder.setCloseUnusedBlocksAfterMillis(100);
        File file = TestFile.createNewFile("media-async-3.blck");
        try (final AsyncContainerMedia media = AsyncContainerMedia.create(file, AsyncContainerMediaProperties.defaultContainerProperties(mediaPropertiesBuilder))) {
            ContainerMeta containerMeta = new ContainerMeta();
            containerMeta.setShardNumber(10);
            long usedBefore = media.getMedia().getMemoryManagement().getPayloadSize();
            long freeBefore = media.getMedia().getMemoryManagement().getLockedFreeSize();
            try (final AsyncContainer asyncContainer = media.getAsyncContainers().create("central", containerMeta)) {

                Map<String, List<String>> created = new HashMap<>();
                Map<String, List<Future<?>>> writeFutures = new HashMap<>();
                Map<String, List<Future<?>>> readFutures = new HashMap<>();
                Map<String, List<Future<?>>> delFutures = new HashMap<>();

                for (int i = 0; i < 100; i++) {
                    String key = getRandomString(10, 50);
                    System.out.println(key);
                    List<String> variants = new ArrayList<>(100);
                    created.put(key, variants);
                    for(int j = 0; j < 100; j++) {
                        if (j == 0) {
                            variants.add(getRandomString(1000, 15000));
                        } else {
                            double selector = Math.random();
                            if (selector < 0.3) {
                                variants.add("DEL");
                            } else {
                                if (selector < 0.6) {
                                    variants.add("READ");
                                } else {
                                    variants.add(getRandomString(1000, 15000));
                                }
                            }
                        }
                    }
                }
                System.out.println("INITIAL WRITING!");
                for (final Map.Entry<String, List<String>> entry : created.entrySet()) {
                    String value = entry.getValue().stream().findFirst().get();
                    List<Future<?>> fs = writeFutures.get(entry.getKey());
                    if (fs == null) {
                        fs = new ArrayList<>();
                        writeFutures.put(entry.getKey(), fs);
                    }
                    fs.add(put(asyncContainer, entry.getKey(), value));
                }
                System.out.println("INITIAL WRITING WAIT!");
                for (Map.Entry<String, List<Future<?>>> resultEntry : writeFutures.entrySet()) {
                    for (Future<?> result : resultEntry.getValue()) {
                        result.get();
                    }
                }
                writeFutures.clear();
                System.out.println("SECONDARY BOMBING!");
                for (final Map.Entry<String, List<String>> entry : created.entrySet()) {
                    entry.getValue().forEach(value -> {
                        if ("DEL".equals(value)) {
                            List<Future<?>> fs = delFutures.get(entry.getKey());
                            if (fs == null) {
                                fs = new ArrayList<>();
                                delFutures.put(entry.getKey(), fs);
                            }
                            fs.add(remove(asyncContainer, entry.getKey()));
                        } else {
                            if ("READ".equals(value)) {
                                List<Future<?>> fs = readFutures.get(entry.getKey());
                                if (fs == null) {
                                    fs = new ArrayList<>();
                                    readFutures.put(entry.getKey(), fs);
                                }
                                fs.add(get(asyncContainer, entry.getKey()));
                            } else {
                                List<Future<?>> fs = writeFutures.get(entry.getKey());
                                if (fs == null) {
                                    fs = new ArrayList<>();
                                    writeFutures.put(entry.getKey(), fs);
                                }
                                fs.add(put(asyncContainer, entry.getKey(), value));
                            }
                        }
                    });
                }
                for (Map.Entry<String, List<Future<?>>> resultEntry : writeFutures.entrySet()) {
                    for (Future<?> result : resultEntry.getValue()) {
                        result.get();
                    }
                }
                for (Map.Entry<String, List<Future<?>>> resultEntry : delFutures.entrySet()) {
                    for (Future<?> result : resultEntry.getValue()) {
                        result.get();
                    }
                }
                for (Map.Entry<String, List<Future<?>>> resultEntry : readFutures.entrySet()) {
                    for(Future<?> result : resultEntry.getValue()) {
                        if (result instanceof AsyncStream) {
                            final AsyncStream<ByteArrayOutputStream> asyncStream = (AsyncStream<ByteArrayOutputStream>) result;
                            String s = new String(asyncStream.get().toByteArray(), StandardCharsets.UTF_8);
                            if (!asyncStream.isCancelled()) {
                                Assertions.assertEquals(true, created.get(resultEntry.getKey()).contains(s));
                            }
                        }
                    }
                }
            }
            Thread.sleep(200);
            media.getMedia().getMappedPhysicalBlocks().closeUnused();
            Assertions.assertEquals(0, media.getMedia().getMemoryConsumption().getBlocksAllocated());

            boolean removed = media.getAsyncContainers().remove("central");
            Assertions.assertEquals(true, removed);
            Thread.sleep(200);
            media.getMedia().getMappedPhysicalBlocks().closeUnused();

            Assertions.assertEquals(2, media.getMedia().getMemoryConsumption().getBlockContainerAllocated());
            Assertions.assertEquals(freeBefore, media.getMedia().getMemoryManagement().getLockedFreeSize());
            Assertions.assertEquals(usedBefore, media.getMedia().getMemoryManagement().getPayloadSize());
        }
    }

    @Test
    public void syncOps() {
        MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
        mediaPropertiesBuilder.setMaxTotalSize(4096L * 1000000L);
        mediaPropertiesBuilder.setBlockSize(4096);
        mediaPropertiesBuilder.setCloseUnusedBlocksAfterMillis(100);
        File file = TestFile.createNewFile("media-async-sync.blck");
        try (final AsyncContainerMedia media = AsyncContainerMedia.create(file, AsyncContainerMediaProperties.defaultContainerProperties(mediaPropertiesBuilder))) {
            ContainerMeta containerMeta = new ContainerMeta();
            containerMeta.setShardNumber(10);
            try (final AsyncContainer asyncContainer = media.getAsyncContainers().create("central", containerMeta)) {
                String bobo = "bobo";
                asyncContainer.put(1, "--", bobo);
                String read = asyncContainer.get(1, "--", String.class).getData();
                Assertions.assertEquals("bobo", read);
            }
            final AsyncContainer asyncContainer = media.getAsyncContainers().get("central");
            Assertions.assertNotNull(asyncContainer);
            media.getAsyncContainers().remove("central");
            final AsyncContainer removedAsyncContainer = media.getAsyncContainers().get("central");
            Assertions.assertNull(removedAsyncContainer);
        } catch (RuntimeException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void syncOpsOpenClose() {
        MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
        mediaPropertiesBuilder.setMaxTotalSize(4096L * 1000000L);
        mediaPropertiesBuilder.setBlockSize(4096);
        mediaPropertiesBuilder.setCloseUnusedBlocksAfterMillis(100);
        File file = TestFile.createNewFile("media-async-sync-op-cl.blck");
        try (final AsyncContainerMedia media = AsyncContainerMedia.create(file, AsyncContainerMediaProperties.defaultContainerProperties(mediaPropertiesBuilder))) {
            ContainerMeta containerMeta = new ContainerMeta();
            containerMeta.setShardNumber(10);
            try (final AsyncContainer asyncContainer = media.getAsyncContainers().create("central", containerMeta)) {
                String bobo = "bobo";
                asyncContainer.put(1, "--", bobo);
            }
        }

        try (final AsyncContainerMedia media = AsyncContainerMedia.load(file)) {
            try (final AsyncContainer asyncContainer = media.getAsyncContainers().get("central")) {
                String bobo = asyncContainer.get(1, "--", String.class).getData();
                Assertions.assertEquals("bobo", bobo);
                DataWithRecord<String> nonExisting = asyncContainer.get(1, "--2", String.class);
                Assertions.assertNull(nonExisting);
            }
        }

    }

    private static String getRandomString(int lengthMin, int lengthMax) {
        final int length = (int)(Math.random() * (lengthMax - lengthMin + 1) + lengthMin);
        final StringBuilder sb = new StringBuilder();
        for(int i=0; i<length; i++) {
            int code = (int)(Math.random() * (26*2+10));
            if (code < 26) {
                code += 'a';
            } else {
                code -= 26;
                if (code < 26) {
                    code += 'A';
                } else {
                    code -= 26;
                    code += '0';
                }
            }
            sb.append((char) code);
        }
        return sb.toString();
    }

    private static Future<?> put(final AsyncContainer container, final String key, final String value) {
        final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
        AsyncStream<InputStream> inputStreamAsyncStream = AsyncStream.wrapBlocking(byteArrayInputStream);
        container.putAsync(1, key.getBytes(StandardCharsets.UTF_8), inputStreamAsyncStream);
        return inputStreamAsyncStream;
    }


    private static Future<?> get(AsyncContainer container, final String key) {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final AsyncStream<OutputStream> byteArrayOutputStream = AsyncStream.wrapBlocking(outputStream);
        container.getAsync(1, key.getBytes(StandardCharsets.UTF_8), byteArrayOutputStream);
        return byteArrayOutputStream;
    }

    private static String getAndWait(AsyncContainer container, final String key) {
        final Future<?> async = get(container, key);
        ByteArrayOutputStream byteArrayOutputStream;
        try {
            byteArrayOutputStream = (ByteArrayOutputStream)async.get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return new String(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8);
    }

    private static Future<Boolean> remove(AsyncContainer container, final String key) {
        return container.removeAsync(1, key.getBytes(StandardCharsets.UTF_8), new Record());
    }
}
