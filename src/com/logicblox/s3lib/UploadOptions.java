package com.logicblox.s3lib;

import com.google.common.base.Optional;

import java.io.File;

/**
 * {@code UploadOptions} contains all the details needed by the upload
 * operation. The specified {@code file} will be uploaded under the specified
 * {@code bucket} and {@code objectKey}.
 * <p/>
 * The specified {@code acl} is applied to the uploaded file.
 * <p/>
 * If the {@code enckey} is present, the {@code keyProvider} will be asked to
 * provide a public key with that name. This key will be used to encrypt the
 * {@code file} at the client side.
 * <p/>
 * If progress listener factory has been set, then progress notifications
 * will be recorded.
 * <p/>
 * {@code UploadOptions} objects are meant to be built by {@code
 * UploadOptionsBuilder}. This class provides only public getter methods.
 */
public class UploadOptions {
    private File file;
    private String bucket;
    private String objectKey;
    private Optional<String> encKey;
    private Optional<String> acl;
    private Optional<OverallProgressListenerFactory>
        overallProgressListenerFactory;

    UploadOptions(File file,
                  String bucket,
                  String objectKey,
                  Optional<String> encKey,
                  Optional<String> acl,
                  Optional<OverallProgressListenerFactory>
                      overallProgressListenerFactory) {
        this.file = file;
        this.bucket = bucket;
        this.objectKey = objectKey;
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
