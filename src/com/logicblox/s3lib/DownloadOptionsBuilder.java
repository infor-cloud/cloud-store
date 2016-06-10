package com.logicblox.s3lib;

import com.google.common.base.Optional;

import java.io.File;
import java.net.URI;

/**
 * {@code DownloadOptionsBuilder} is a builder for {@code DownloadOptions}
 * objects.
 * <p>
 * Setting fields {@code file}, {@code bucket} and {@code objectKey} is
 * mandatory. All the others are optional.
 */
public class DownloadOptionsBuilder {
    private File file;
    private String bucket;
    private String objectKey;
    private String version;
    private boolean recursive = false;
   
    private boolean overwrite = false;
    private Optional<OverallProgressListenerFactory> overallProgressListenerFactory =
        Optional.absent();

    public DownloadOptionsBuilder setFile(File file) {
        this.file = file;
        return this;
    }

    public DownloadOptionsBuilder setBucket(String bucket) {
        this.bucket = bucket;
        return this;
    }

    public DownloadOptionsBuilder setObjectKey(String objectKey) {
        this.objectKey = objectKey;
        return this;
    }

    public DownloadOptionsBuilder setUri(URI uri) {
        setBucket(Utils.getBucket(uri)).setObjectKey(Utils.getObjectKey(uri));
        return this;
    }

    public DownloadOptionsBuilder setRecursive(boolean recursive) {
        this.recursive = recursive;
        return this;
    }
    
    public DownloadOptionsBuilder setVersion(String version) {
        this.version = version;
        return this;
    }
    

    public DownloadOptionsBuilder setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public DownloadOptionsBuilder setOverallProgressListenerFactory
        (OverallProgressListenerFactory overallProgressListenerFactory) {
        this.overallProgressListenerFactory = Optional.fromNullable
            (overallProgressListenerFactory);
        return this;
    }

    public DownloadOptions createDownloadOptions() {
        return new DownloadOptions(file, bucket, objectKey, version,recursive,
             overwrite, overallProgressListenerFactory);
    }
}