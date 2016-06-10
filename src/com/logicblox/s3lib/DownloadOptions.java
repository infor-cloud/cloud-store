package com.logicblox.s3lib;

import com.google.common.base.Optional;

import java.io.File;

/**
 * {@code DownloadOptions} contains all the details needed by the download
 * operation. The specified {@code object}, under {@code bucket}, is downloaded
 * to a local {@code file}.
 * <p>
 * If {@code recursive} is set, then all objects under {@code object} key will
 * be downloaded. Otherwise, only the first-level objects will be downloaded.
 * <p>
 * If {@code overwrite} is set, then newly downloaded files is possible to
 * overwrite existing local files.
 * <p>
 * If progress listener factory has been set, then progress notifications
 * will be recorded.
 * <p>
 * {@code DownloadOptions} objects are meant to be built by {@code
 * DownloadOptionsBuilder}. This class provides only public getter methods.
 */
public class DownloadOptions {
    private File file;
    private String bucket;
    private String objectKey;
    private boolean recursive;
    private String version;
    private boolean overwrite;
    private Optional<OverallProgressListenerFactory>
        overallProgressListenerFactory;

    DownloadOptions(File file,
                    String bucket,
                    String objectKey,
                    String version,
                    boolean recursive,
                    boolean overwrite,
                    Optional<OverallProgressListenerFactory>
                        overallProgressListenerFactory) {
        this.file = file;
        this.bucket = bucket;
        this.objectKey = objectKey;
        this.recursive = recursive;
        this.version = version;
        this.overwrite = overwrite;
        this.overallProgressListenerFactory = overallProgressListenerFactory;
    }

    public File getFile() {
        return file;
    }

    public String getBucket() {
        return bucket;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public boolean isRecursive() {
        return recursive;
    }
    public String getVersion() {
        return version;
    }

    public boolean doesOverwrite() {
        return overwrite;
    }

    public Optional<OverallProgressListenerFactory>
    getOverallProgressListenerFactory() {
        return overallProgressListenerFactory;
    }
}
