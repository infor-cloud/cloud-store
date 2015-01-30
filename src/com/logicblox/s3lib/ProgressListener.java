package com.logicblox.s3lib;

import com.google.common.base.Stopwatch;

import java.text.MessageFormat;

abstract class ProgressListener {
    protected final Stopwatch stopwatch = Stopwatch.createUnstarted();

    protected final long intervalInBytes;
    protected final String reportName;
    protected final String operation;
    protected final long totalSizeInBytes;
    protected long bytesTransferred = 0L;
    protected long lastReportBytes = 0L;


    ProgressListener(String reportName, String operation, long intervalInBytes, long totalSizeInBytes) {
        this.reportName = reportName;
        this.operation = operation;
        this.intervalInBytes = intervalInBytes;
        this.totalSizeInBytes = totalSizeInBytes;
    }

    void started() {
        stopwatch.start();
        System.out.println(MessageFormat.format("{0} of {1} started...",
                operation, reportName));
    }

    /**
     * Used for listeners that report number of bytes per transfer.
     */
    void progress(long numBytesTransferred) {
        _progressCum(bytesTransferred + numBytesTransferred);
    }

    /**
     * Used for listeners that report cumulative number of transferred bytes.
     */
    void progressCum(long numBytesTransferred) {
        _progressCum(numBytesTransferred);
    }

    private void _progressCum(long cumBytesTransferred) {
        if (((cumBytesTransferred - lastReportBytes) >= intervalInBytes) ||
                (cumBytesTransferred == totalSizeInBytes)) {
            if (totalSizeInBytes >= 0) {
                System.out.println(MessageFormat.format(
                                "{0}: ({1}%) {2}ed {3}/{4} bytes...",
                                reportName,
                                100 * cumBytesTransferred / totalSizeInBytes,
                                operation,
                                cumBytesTransferred,
                                totalSizeInBytes)
                );
            } else {
                System.out.println(MessageFormat.format("{0}: {1}ed {2} bytes...",
                        reportName, operation, cumBytesTransferred));
            }
            lastReportBytes = cumBytesTransferred;
        }
        bytesTransferred = cumBytesTransferred;
    }

    void complete() {
        if (stopwatch.isRunning()) {
            stopwatch.stop();
            System.out.println(MessageFormat.format("{0} of {1} completed in {2}",
                    operation, reportName, stopwatch));
        } else {
            System.out.println(MessageFormat.format("{0} of {1} completed",
                    operation, reportName));
        }
    }
}
