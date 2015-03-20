package com.logicblox.s3lib;

import java.util.concurrent.atomic.AtomicLong;

public class PartProgressEvent {
    final String partId;
    private AtomicLong lastTransferBytes = new AtomicLong();
    private AtomicLong transferredBytes = new AtomicLong();

    public PartProgressEvent(String partId) {
        this.partId = partId;
    }

    public String getPartId() {
        return partId;
    }

    public long getLastTransferBytes() {
        return lastTransferBytes.get();
    }

    public void setLastTransferBytes(long lastTransferBytes) {
        this.lastTransferBytes.set(lastTransferBytes);
        this.transferredBytes.addAndGet(lastTransferBytes);
    }

    public long getTransferredBytes() {
        return transferredBytes.get();
    }

    public void setTransferredBytes(long transferredBytes) {
        this.transferredBytes.set(transferredBytes);
    }
}
