package com.logicblox.s3lib;

import com.google.common.base.Optional;

import java.io.File;
import java.net.URI;

/**
 * {@code DownloadOptionsBuilder} is a builder for {@code DownloadOptions}
 * objects.
 * <p/>
 * Setting fields {@code file}, {@code bucket} and {@code objectKey} is
 * mandatory. All the others are optional.
 */
public class DownloadOptionsBuilder {
    private File file;
    private String bucket;
    private String objectKey;
    private boolean recursive = false;
    private boolean overwrite = false;
    private Optional<S3ProgressListenerFactory> s3ProgressListenerFactory =
        Optional.absent();
    private Optional<GCSProgressListenerFactory> gcsProgressListenerFactory =
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

    public DownloadOptionsBuilder setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
        return this;
    }

    public DownloadOptionsBuilder setS3ProgressListenerFactory
        (S3ProgressListenerFactory s3ProgressListenerFactory) {
        this.s3ProgressListenerFactory = Optional.fromNullable
            (s3ProgressListenerFactory);
        return this;
    }

    public DownloadOptionsBuilder setGcsProgressListenerFactory(
        GCSProgressListenerFactory gcsProgressListenerFactory) {
        this.gcsProgressListenerFactory = Optional.fromNullable
            (gcsProgressListenerFactory);
        return this;
    }

    public DownloadOptions createDownloadOptions() {
        return new DownloadOptions(file, bucket, objectKey, recursive,
            overwrite, s3ProgressListenerFactory, gcsProgressListenerFactory);
    }
}