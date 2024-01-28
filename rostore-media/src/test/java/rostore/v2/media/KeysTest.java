package rostore.v2.media;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.rostore.entity.media.MediaPropertiesBuilder;
import rostore.TestFile;
import org.rostore.entity.Record;
import org.rostore.v2.keys.KeyBlockOperations;
import org.rostore.v2.keys.RecordLengths;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.MediaProperties;
import org.rostore.v2.media.block.allocator.BlockAllocator;
import org.rostore.v2.media.block.container.BlockContainer;

import java.io.File;
import java.nio.charset.StandardCharsets;

public class KeysTest {

    private KeyBlockOperations keyBlockOperations;

    @Test
    public void testKeys() {

        File file = TestFile.createNewFile("media-2-keys.blck");
        MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
        mediaPropertiesBuilder.setMaxTotalSize(40*1000);
        mediaPropertiesBuilder.setBlockSize(40);
        Media media = Media.create(file, MediaProperties.from(mediaPropertiesBuilder));

        Assertions.assertEquals(2 *40, media.getBlockAllocation().getPayloadSize());

        BlockContainer bc = media.newBlockContainer();
        BlockAllocator secondaryBlockAllocator = media.createSecondaryBlockAllocator("blah",
                1000);
        keyBlockOperations = KeyBlockOperations.create(secondaryBlockAllocator, RecordLengths.standardRecordLengths(media.getMediaProperties()));
        put("qwerty", 15);
        put("_qwerty", 16);
        put("qwerty123", 17);
        Assertions.assertEquals(get("qwerty"), 15);
        Assertions.assertEquals(get("_qwerty"), 16);
        Assertions.assertEquals(get("qwerty123"), 17);
        Assertions.assertEquals(remove("qwerty"), true);
        Assertions.assertEquals(remove("qwerty"), false);
        Assertions.assertEquals(get("_qwerty"), 16);
        Assertions.assertEquals(get("qwerty123"), 17);
        bc.commit();

        secondaryBlockAllocator.remove();

        Assertions.assertEquals(2 *40, media.getBlockAllocation().getPayloadSize());

    }

    private long put(final String key, final long id) {
        return keyBlockOperations.put(key.getBytes(StandardCharsets.UTF_8), new Record().id(id));
    }

    private long get(final String key) {
        return keyBlockOperations.getRecord(key.getBytes(StandardCharsets.UTF_8)).getId();
    }

    private boolean remove(final String key) {
        return keyBlockOperations.remove(key.getBytes(StandardCharsets.UTF_8), new Record());
    }

}
