package org.rostore.v2.container;

import org.rostore.Utils;
import org.rostore.entity.Record;
import org.rostore.v2.keys.KeyBlockOperations;
import org.rostore.v2.keys.KeyList;
import org.rostore.v2.keys.RecordLengths;
import org.rostore.v2.media.Committable;
import org.rostore.v2.media.block.container.Status;
import org.rostore.v2.container.ContainerShard;
import org.rostore.v2.seq.BlockSequence;


/**
 * Encapsulates the operation context on the shard level.
 *
 * <p>Every key read/write/delete operation should be executed over this class.</p>
 * <p>The objects of this class are not thread safe and have a state, so it should
 * be created just one type of this operation object for every parallel process.</p>
 * <p>Once the key-access operation is over, the object can be reused in another process,
 * but during the process should be exactly one process waiting for it.</p>
 * <p>The key-read operations can be executed in parallel as many as needed.</p>
 * <p>The key-write or key-delete operations should not be executed in parallel.</p>
 * <p>The mechanism to for separating the blocking write and non-blocking read operation should be
 * implemented on the caller side.</p>
 */
public class ContainerShardKeyOperations implements Committable {

    /**
     * The reference to the parent shard
     */
    private final ContainerShard containerShard;

    /**
     * A core key block operations initialized on the shard,
     * it holds also the blocks involved in the operation
     */
    private final KeyBlockOperations keyBlockOperations;

    /**
     * Creates an instance for the shard.
     */
    public ContainerShardKeyOperations(final ContainerShard containerShard) {
        this.containerShard = containerShard;
        this.keyBlockOperations = KeyBlockOperations.load(containerShard.getShardAllocator(),
                containerShard.getDescriptor().getKeysStartIndex(),
                RecordLengths.standardRecordLengths(containerShard.getContainer().getContainerListOperations().getMedia().getMediaProperties()));
    }

    /**
     * The sequence of the blocks that manages the shard's keys
     * @return
     */
    public BlockSequence getBlockSequence() {
        return keyBlockOperations.getBlockSequence();
    }

    /**
     * Create or update the provided key with the parameter from the provided record.
     *
     * <p>Operation will execute the validation for the legitimacy of the update.</p>
     * <p>If the TTL of the record would be higher than the one configured for the container,
     * the TTL in the Record will be corrected.</p>
     *
     * @param record the data to be stored with the key
     * @return the id that has been stored before with the key or {@link Utils#ID_UNDEFINED}
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

    /**
     * Executes an operation to find a key
     *
     * @param key a key to be found
     * @return the {@link Record} associated with the key or {@code null} if the key has not been found
     */
    public Record getKey(final byte[] key) {
        try {
            return keyBlockOperations.getRecord(key);
        } finally {
            keyBlockOperations.commit();
        }
    }

    /**
     * The operation is executed in the clean-up cycle.
     *
     * <p>It will search for the expired records on the block of the sequence and
     * remove it if one found. In the later case the id of the entry is returned.
     * If nothing is found {@link Utils#ID_UNDEFINED} is returned.</p>
     *
     * @param keyBlockSequenceIndex
     * @return an id associated with the deleted entry or {@link Utils#ID_UNDEFINED} if nothing is found.
     */
    public long removeKeyIfExpired(final int keyBlockSequenceIndex) {
        try {
            return keyBlockOperations.removeIfExpired(keyBlockSequenceIndex);
        } finally {
            keyBlockOperations.commit();
        }
    }

    /**
     * Removes a key from the shard
     *
     * @param key a key to rmeove
     * @param record the metadata to be used
     * @return {@code true} if deletion happened or {@code false} otherwise
     */
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
