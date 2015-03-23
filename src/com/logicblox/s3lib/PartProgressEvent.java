package com.logicblox.s3lib;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Notification of a progress change on a part transfer. Typically this means
 * notice that another chunk of bytes of the specified part was transferred.
 */
public class PartProgressEvent {
    final String partId;
    private AtomicLong lastTransferBytes = new AtomicLong();
    private AtomicLong transferredBytes = new AtomicLong();

    PartProgressEvent(String partId) {
        this.partId = partId;
    }

    public String getPartId() {
        return partId;
    }

    /**
     * We declare it as {@code synchronized} because, typically, it can be
     * called by two threads that transfer different chunks of the same part.
     */
    synchronized public void setLastTransferBytes(long lastTransferBytes) {
        this.lastTransferBytes.set(lastTransferBytes);
        this.transferredBytes.addAndGet(lastTransferBytes);
    }

    public void setTransferredBytes(long transferredBytes) {
        this.transferredBytes.set(transferredBytes);
    }

    public long getTransferredBytes() {
        return transferredBytes.get();
    }
}
