package org.rostore;

import java.nio.charset.Charset;

/**
 * General functions and constants used in rostore modules.
 */
public class Utils {

    public final static Charset UTF_8 = Charset.forName("UTF-8");

    private final static long TTL_START=1595776424;
    public final static int BYTES_PER_EOL = 4;
    public final static int BYTES_PER_VERSION = 1;
    public final static long TTL_FOREVER = 0;
    public final static long TTL_EXPIRED = 0;
    public final static long EOL_FOREVER = 0;
    public final static long VERSION_UNDEFINED = 0;
    public final static long VERSION_START = VERSION_UNDEFINED+1;
    public final static long ID_UNDEFINED = -1;

    public static byte[] getBytes(final String str) {
        return str==null?null:str.getBytes(UTF_8);
    }

    /**
     * Converts the time-to-live to end-of-live time
     * @param ttl in seconds
     * @return eol
     */
    public final static long ttl2eol(final long ttl) {
        if (ttl == TTL_FOREVER) {
            return EOL_FOREVER;
        }
        return System.currentTimeMillis()/1000 - TTL_START + ttl;
    }

    public final static long ttl2unixEol(final long ttl) {
        if (ttl == TTL_FOREVER) {
            return EOL_FOREVER;
        }
        return (System.currentTimeMillis() / 1000 + ttl) * 1000;
    }

    /**
     *
     * @param eol unix time in seconds starting from {@link #TTL_START}
     * @return ttl in seconds
     */
    public final static long eol2ttl(final long eol) {
        if (eol == EOL_FOREVER) {
            return TTL_FOREVER;
        }
        long ttl = (eol + TTL_START) - System.currentTimeMillis()/1000;
        if (ttl <= 0) {
            return TTL_EXPIRED;
        }
        return ttl;
    }

    public final static long eol2unix(final long eol) {
        if (eol == 0) {
            return 0;
        }
        return (eol + TTL_START) * 1000;
    }

    public final static boolean isExpiredEOL(long eol) {
        return isExpiredEOL(eol, System.currentTimeMillis()/1000);
    }

    public final static boolean isExpiredEOL(final long eol, final long currentTimeSecs) {
        if (eol == EOL_FOREVER) {
            return false;
        }
        return eol + TTL_START < currentTimeSecs;
    }

    /**
     * Computes how many bytes is needed to represent the value
     *
     * @param maxValue the value can only be in range 0..maxValue
     * @return
     */
    public static int computeBytesForMaxValue(long maxValue) {
        int byteNumber = 0;
        for(int i=0; i<8; i++) {
            long mask = 0xff;
            mask <<= i*8;
            mask &= maxValue;
            if (mask != 0) {
                byteNumber=i+1;
            }
        }
        return byteNumber;
    }

    public static long trimByBytes(long value, int bytes) {
        long ret = 0;
        long mask = 0xff;
        for(int i=0; i<bytes; i++) {
            ret |= value & mask;
            mask <<= 8;
        }
        return ret;
    }

    public static long unixEol2eol(long unixEol) {
        return unixEol / 1000 - TTL_START;
    }
}
