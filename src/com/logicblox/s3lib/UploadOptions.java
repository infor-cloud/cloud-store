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
 * to more than 10000 parts will happen later.
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
public class UploadOptions extends CommandOptions {
    private File file;
    private String bucket;
    private String objectKey;
    private long chunkSize = -1;
    private Optional<String> encKey;
    private Optional<String> acl;
    private boolean dryRun;
    private boolean ignoreAbortInjection;
    private Optional<OverallProgressListenerFactory>
        overallProgressListenerFactory;

    // for testing
    private static AbortCounters _abortCounters = new AbortCounters();


    UploadOptions(CloudStoreClient cloudStoreClient,
                  File file,
                  String bucket,
                  String objectKey,
                  long chunkSize,
                  Optional<String> encKey,
                  Optional<String> acl,
                  boolean dryRun,
                  boolean ignoreAbortInjection,
                  Optional<OverallProgressListenerFactory>
                      overallProgressListenerFactory) {
        super(cloudStoreClient);
        this.file = file;
        this.bucket = bucket;
        this.objectKey = objectKey;
        this.chunkSize = chunkSize;
        this.encKey = encKey;
        this.acl = acl;
        this.dryRun = dryRun;
        this.ignoreAbortInjection = ignoreAbortInjection;
        this.overallProgressListenerFactory = overallProgressListenerFactory;
    }


    // for testing injection of aborts during a copy
    void injectAbort(String id)
    {
      if(!this.ignoreAbortInjection
           && (_abortCounters.decrementInjectionCounter(id) > 0))
      {
        throw new AbortInjection("forcing upload abort");
      }
    }

    static AbortCounters getAbortCounters()
    {
      return _abortCounters;
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

    public long getChunkSize() {
        if (file.isDirectory()) {
            return -1;
        }
        if (chunkSize == -1) {
            return Utils.getDefaultChunkSize(file.length());
        }
        return chunkSize;
    }

    public Optional<String> getEncKey() {
        return encKey;
    }

    public Optional<String> getAcl() {
        if (!acl.isPresent()) {
            if (getCloudStoreClient().getScheme().equals("s3")) {
                acl = Optional.of("bucket-owner-full-control");
            }
            else if (getCloudStoreClient().getScheme().equals("gs")) {
                acl = Optional.of("projectPrivate");
            }
        }
        return acl;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public Optional<OverallProgressListenerFactory>
    getOverallProgressListenerFactory() {
        return overallProgressListenerFactory;
    }
}
