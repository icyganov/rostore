package rostore.v2.media.data;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.rostore.v2.data.DataReader;
import org.rostore.v2.data.DataWriter;
import org.rostore.entity.media.MediaPropertiesBuilder;
import org.rostore.v2.media.block.allocator.BlockAllocator;
import org.rostore.v2.media.Media;
import org.rostore.v2.media.MediaProperties;
import rostore.TestFile;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class DataOperationsTest {

    @Test
    public void test() {
        File file = TestFile.createNewFile("media-data.blck");
        MediaPropertiesBuilder mediaPropertiesBuilder = new MediaPropertiesBuilder();
        mediaPropertiesBuilder.setMaxTotalSize(40*100000);
        mediaPropertiesBuilder.setBlockSize(40);
        Media media = Media.create(file, MediaProperties.from(mediaPropertiesBuilder));
        BlockAllocator blockAllocator = media.createSecondaryBlockAllocator("blah", 100000);

        Map<Integer, Long> map = new HashMap<>();

        for(int j=0; j<40*10 + 89; j++) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < j; i++) {
                sb.append((i % 2) == 0 ? "a" : "b");
            }
            String s = sb.toString();
            long id = DataWriter.writeObject(blockAllocator, s);
            map.put(j, id);
        }

        for(int j=0; j<40*10 + 89; j++) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < j; i++) {
                sb.append((i % 2) == 0 ? "a" : "b");
            }
            String s = sb.toString();
            long id = map.get(j);
            String fromStroage = DataReader.readObject(media, id, String.class);
            Assertions.assertEquals(s, fromStroage);
        }
    }
}
