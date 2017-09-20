package com.logicblox.s3lib;

import java.io.File;
import java.util.Optional;

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
public class DownloadOptions extends CommandOptions {
    private File file;
    private String bucket;
    private String objectKey;
    private boolean recursive;
    private String version;
    private boolean overwrite;
    private boolean dryRun;
    private OverallProgressListenerFactory overallProgressListenerFactory;

    DownloadOptions(CloudStoreClient cloudStoreClient,
                    File file,
                    String bucket,
                    String objectKey,
                    String version,
                    boolean recursive,
                    boolean overwrite,
                    boolean dryRun,
                    OverallProgressListenerFactory
                        overallProgressListenerFactory) {
        super(cloudStoreClient);
        this.file = file;
        this.bucket = bucket;
        this.objectKey = objectKey;
        this.recursive = recursive;
        this.version = version;
        this.overwrite = overwrite;
        this.dryRun = dryRun;
        this.overallProgressListenerFactory = overallProgressListenerFactory;
    }

    public File getFile() {
        return file;
    }

    public String getBucketName() {
        return bucket;
    }

    public String getObjectKey() {
        return objectKey;
    }

    public boolean isRecursive() {
        return recursive;
    }
    
    public Optional<String> getVersion() {
        return Optional.ofNullable(version);
    }

    public boolean doesOverwrite() {
        return overwrite;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public Optional<OverallProgressListenerFactory>
    getOverallProgressListenerFactory() {
        return Optional.ofNullable(overallProgressListenerFactory);
    }
}
