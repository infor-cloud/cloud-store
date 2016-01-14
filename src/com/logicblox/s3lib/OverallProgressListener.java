package com.logicblox.s3lib;

/**
 * Listener interface for part-transfer progress events.
 */
public interface OverallProgressListener {
    /**
     * Called to notify that progress has been changed for a specific part of
     * the transferred file.
     * <p>
     * This methods might be called from multiple different threads, each one
     * transferring different part of the file, so it has to be implemented in a
     * thread-safe manner. Declaring it as {@code synchronized} is suggested.
     */
    public void progress(PartProgressEvent partProgressEvent);
}
