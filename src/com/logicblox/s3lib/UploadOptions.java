package com.logicblox.s3lib;

import com.google.common.base.Optional;

import java.io.File;

/**
 * {@code UploadOptions} contains all the details needed by the upload
 * operation. The specified {@code file} will be uploaded under the specified
 * {@code bucket} and {@code objectKey}.
 * <p>
 * If the {@code chunkSize} is {@code null}, then {@code getChunkSize} will try
 * to compute a chunk size so that the number of the uploaded parts be less
 * than 10000 (current S3 limit). If the {@code chunkSize} is explicit (i.e.
 * not {@code null}, then no check will take place and any possible failure due
 * to >10000 parts will happen later.
 * <p>
 * The specified {@code acl} is applied to the uploaded file.
 * <p>
 * If the {@code enckey} is present, the {@code keyProvider} will be asked to
 * provide a public key with that name. This key will be used to encrypt the
 * {@code file} at the client side.
 * <p>
 * If progress listener factory has been set, then progress notifications
 * will be recorded.
 * <p>
 * {@code UploadOptions} objects are meant to be built by {@code
 * UploadOptionsBuilder}. This class provides only public getter methods.
 */
public class UploadOptions {
    private File file;
    private String bucket;
    private String objectKey;
    private Long chunkSize;
    private Optional<String> encKey;
    private Optional<String> acl;
    private Optional<OverallProgressListenerFactory>
        overallProgressListenerFactory;

    UploadOptions(File file,
                  String bucket,
                  String objectKey,
                  Long chunkSize,
                  Optional<String> encKey,
                  Optional<String> acl,
                  Optional<OverallProgressListenerFactory>
                      overallProgressListenerFactory) {
        this.file = file;
        this.bucket = bucket;
        this.objectKey = objectKey;
        this.chunkSize = chunkSize;
        this.encKey = encKey;
        this.acl = acl;
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

    // TODO: Return Optional<Long>
    public Long getChunkSize() {
        if (file.isDirectory()) {
            return null;
        }
        if (chunkSize == null) {
            return Utils.getDefaultChunkSize(file.length());
        }
        return chunkSize;
    }

    public Optional<String> getEncKey() {
        return encKey;
    }

    public Optional<String> getAcl() {
        return acl;
    }

    public Optional<OverallProgressListenerFactory>
    getOverallProgressListenerFactory() {
        return overallProgressListenerFactory;
    }
}
