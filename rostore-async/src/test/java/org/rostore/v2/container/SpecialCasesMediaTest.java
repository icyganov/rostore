package org.rostore.v2.container;

import org.junit.jupiter.api.Test;
import org.rostore.TestFile;
import org.rostore.entity.Record;
import org.rostore.entity.media.ContainerMeta;
import org.rostore.v2.container.async.AsyncContainer;
import org.rostore.v2.container.async.AsyncContainerMedia;
import org.rostore.v2.container.async.AsyncContainerMediaProperties;
import org.rostore.entity.media.MediaPropertiesBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SpecialCasesMediaTest {

    @Test
    public void test() {
        MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
        mediaPropertiesBuilder.setMaxTotalSize(4096*10000);
        mediaPropertiesBuilder.setBlockSize(4096);
        ExecutorService es = Executors.newFixedThreadPool(10);
        File file = TestFile.createNewFile("media-spec.blck");
        try (AsyncContainerMedia media = AsyncContainerMedia.create(file, AsyncContainerMediaProperties.defaultContainerProperties(mediaPropertiesBuilder))) {
            ContainerMeta containerMeta = new ContainerMeta();
            containerMeta.setShardNumber(10);

            List<String> containers = new ArrayList<>();
            List<Future> waitList = new ArrayList<>();
            for (int i=0; i<100; i++) {
                String name = getRandomString(10,50);
                containers.add(name);
                Future<?> res = es.submit(() -> {
                    media.getAsyncContainers().create(name, containerMeta);
                });
                waitList.add(res);
            }

            System.out.println("Wait container creation...");
            waitAll(waitList);
            waitList.clear();

            System.out.println("Start random operation...");
            for(int z=0; z<10; z++) {
                for(int u=0; u<10; u++) {
                    for (final String name : containers) {
                        waitList.add(es.submit(() -> {
                            if (Math.random() < 0.5) {
                                System.out.println("CONT: remove " + name + " start");
                                media.getAsyncContainers().remove(name);
                                System.out.println("CONT: remove " + name + " stop");
                            } else {
                                AsyncContainer asyncContainer = media.getAsyncContainers().get(name);
                                if (asyncContainer != null) {
                                    if (Math.random() < 0.5) {
                                        System.out.println("KEY: add " + name + " start");
                                        asyncContainer.put(0, "tatata", "lalala", new Record());
                                        System.out.println("KEY: add " + name + " stop");
                                    } else {
                                        if (Math.random() < 0.5) {
                                            System.out.println("KEY: remove " + name + " start");
                                            asyncContainer.remove(0, "tatata");
                                            System.out.println("KEY: remove " + name + " stop");
                                        } else {
                                            System.out.println("KEY: get " + name + " start");
                                            asyncContainer.get(0, "tatata", String.class);
                                            System.out.println("KEY: get " + name + " stop");
                                        }
                                    }
                                } else {
                                    System.out.println("CONT: create " + name + " start");
                                    media.getAsyncContainers().create(name, containerMeta);
                                    System.out.println("CONT: create " + name + " stop");
                                }
                            }
                        }));
                    }
                    System.out.println("iteration " + z);
                }
                waitAll(waitList);
                waitList.clear();
            }

            es.shutdown();
        }
    }

    private static void waitAll(final List<Future> futures) {
        try {
            for(Future f : futures) {
                f.get();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
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

}
