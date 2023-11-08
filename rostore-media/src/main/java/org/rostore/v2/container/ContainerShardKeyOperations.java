package org.rostore.v2.container;

import org.rostore.entity.Record;
import org.rostore.v2.keys.KeyBlockOperations;
import org.rostore.v2.keys.KeyList;
import org.rostore.v2.keys.RecordLengths;
import org.rostore.v2.media.Committable;
import org.rostore.v2.media.block.container.Status;
import org.rostore.v2.container.ContainerShard;
import org.rostore.v2.seq.BlockSequence;


public class ContainerShardKeyOperations implements Committable {

    private final ContainerShard containerShard;
    private final KeyBlockOperations keyBlockOperations;

    /**
     * Loader
     */
    public ContainerShardKeyOperations(final ContainerShard containerShard) {
        this.containerShard = containerShard;
        this.keyBlockOperations = KeyBlockOperations.load(containerShard.getShardAllocator(),
                containerShard.getDescriptor().getKeysStartIndex(),
                RecordLengths.standardRecordLengths(containerShard.getContainer().getContainerListOperations().getMedia().getMediaProperties()));
    }

    public BlockSequence getBlockSequence() {
        return keyBlockOperations.getBlockSequence();
    }

    /**
     *
     * @param record
     * @return true if new key has been added, false if the old one has been replaced
     */
    public long putKey(final byte[] key, final Record record) {
        final long maxTTL = containerShard.getContainer().getDescriptor().getContainerMeta().getMaxTTL();
        if (maxTTL != 0 && (record.getTtl() == 0 || record.getTtl() > maxTTL)) {
            record.ttl(maxTTL);
        }
        try {
            return keyBlockOperations.put(key, record);
        } finally {
            keyBlockOperations.commit();
        }
    }

       public KeyList listKeys(final byte[] startWithKey, final byte[] continuationKey, int maxNumber, int maxSize) {
        try {
            return keyBlockOperations.list(startWithKey, continuationKey, maxNumber, maxSize);
        } finally {
            keyBlockOperations.commit();
        }
    }

    public Record getKey(final byte[] key) {
        try {
            return keyBlockOperations.getRecord(key);
        } finally {
            keyBlockOperations.commit();
        }
    }

    /**
     * or ID_UNDEFINED
     * @param keyBlockSequenceIndex
     * @return
     */
    public long removeKeyIfExpired(final int keyBlockSequenceIndex) {
        try {
            return keyBlockOperations.removeIfExpired(keyBlockSequenceIndex);
        } finally {
            keyBlockOperations.commit();
        }
    }

    public boolean removeKey(final byte[] key, final Record record) {
        try {
            return keyBlockOperations.remove(key, record);
        } finally {
            keyBlockOperations.commit();
        }
    }

    public void dump() {
        System.out.println("--- keys ---");
        keyBlockOperations.dump();
    }

    @Override
    public void close() {
        keyBlockOperations.close();
    }

    @Override
    public Status getStatus() {
        return null;
    }

    @Override
    public void commit() {
        keyBlockOperations.commit();
    }
}
