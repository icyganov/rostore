package rostore;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class TestFile {

    private final static boolean HAS_BLOCKDEVICE = false;

    private final static Set<String> usedNames = new HashSet<>(1000);

    private final static File TEST_ROOT_DIR=new File("/home/miner/tests");
    private final static File TEST_BLOCKDEVICE=new File("/dev/sda4");

    public static File createNewFile(final String shortName) {
        if (usedNames.contains(shortName)) {
            throw new IllegalStateException("The name " + shortName + " is already in use.");
        }
        usedNames.add(shortName);
        if (!TEST_ROOT_DIR.exists()) {
            if (!TEST_ROOT_DIR.mkdirs()) {
                throw new IllegalStateException("Can't create " + TEST_ROOT_DIR);
            }
        }
        File file = new File(TEST_ROOT_DIR, shortName);
        if (file.exists()) {
            file.delete();
        }
        return file;
    }

    public static File getBlockDevice() {
        return TEST_BLOCKDEVICE;
    }

    public static boolean hasBlockdevice() {
        return HAS_BLOCKDEVICE;
    }
}
