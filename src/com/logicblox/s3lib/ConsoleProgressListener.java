package com.logicblox.s3lib;

import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

class ConsoleProgressListener implements OverallProgressListener {
    protected ConcurrentMap<String, PartProgressEvent> partsProgressEvents =
        new ConcurrentHashMap<String, PartProgressEvent>();
    protected final long intervalInBytes;
    protected final String reportName;
    protected final String operation;
    protected final long totalSizeInBytes;
    protected AtomicLong lastReportBytes = new AtomicLong();

    ConsoleProgressListener(String reportName, String operation, long
        intervalInBytes, long totalSizeInBytes) {
        this.reportName = reportName;
        this.operation = operation;
        this.intervalInBytes = intervalInBytes;
        this.totalSizeInBytes = totalSizeInBytes;
    }

    synchronized public void progress(PartProgressEvent partProgressEvent) {
        partsProgressEvents.put(partProgressEvent.getPartId(),
            partProgressEvent);

        long totalTransferredBytes = getTotalTransferredBytes();
        long unreportedBytes = getUreportedBytes(totalTransferredBytes);
        if (isReportTime(unreportedBytes) ||
            (isComplete(totalTransferredBytes) && !allBytesReported())) {
            System.out.println(MessageFormat.format(
                "{0}: ({1}%) {2}ed {3}/{4} bytes...",
                reportName,
                100 * totalTransferredBytes / totalSizeInBytes,
                operation,
                totalTransferredBytes,
                totalSizeInBytes));
            lastReportBytes.set(totalTransferredBytes);
        }
    }

    private long getTotalTransferredBytes() {
        AtomicLong bytes = new AtomicLong();
        for (Map.Entry<String, PartProgressEvent> e :
            partsProgressEvents.entrySet()) {
            bytes.addAndGet(e.getValue().getTransferredBytes());
        }

        return bytes.get();
    }

    private long getUreportedBytes(long totalTransferredBytes) {
        return totalTransferredBytes - lastReportBytes.get();
    }

    private boolean isReportTime(long unreportedBytes) {
        return unreportedBytes >= intervalInBytes;
    }

    private boolean allBytesReported() {
        return lastReportBytes.get() == totalSizeInBytes;
    }

    private boolean isComplete(long totalTransferredBytes) {
        return totalTransferredBytes == totalSizeInBytes;
    }
}
