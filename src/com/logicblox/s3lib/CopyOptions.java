package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.AccessControlList;
import com.google.common.base.Optional;
import java.util.Map;


/**
 * {@code CopyOptions} contains all the details needed by the copy operation.
 * The specified {@code sourceKey}, under {@code sourceBucketName} bucket, is
 * copied
 * to {@code destinationKey}, under {@code destinationBucketName}.
 * <p>
 * If {@code cannedAcl} is specified then it's applied to the destination
 * object.
 * <p>
 * If progress listener factory has been set, then progress notifications will
 * be recorded.
 * <p>
 * {@code CopyOptions} objects are meant to be built by {@code
 * CopyOptionsBuilder}. This class provides only public getter methods.
 */
public class CopyOptions extends CommandOptions {
    private final String sourceBucketName;
    private final String sourceKey;
    private final String destinationBucketName;
    private final String destinationKey;
    private final boolean recursive;
    private final boolean dryRun;
    private final boolean ignoreAbortInjection;
    // TODO(geo): Revise use of Optionals. E.g. it's not a good idea to use them
    // as fields.
    private Optional<String> cannedAcl;
    private final Optional<AccessControlList> s3Acl;
    private final String storageClass;
    private final Optional<Map<String,String>> userMetadata;
    private final Optional<OverallProgressListenerFactory>
        overallProgressListenerFactory;

    // for testing injection of aborts during a copy
    private static AbortCounters _abortCounters = new AbortCounters();


    CopyOptions(CloudStoreClient cloudStoreClient,
                String sourceBucketName,
                String sourceKey,
                String destinationBucketName,
                String destinationKey,
                Optional<String> cannedAcl,
                Optional<AccessControlList> s3Acl,
                String storageClass,
                boolean recursive,
                boolean dryRun,
                boolean ignoreAbortInjection,
                Optional<Map<String,String>> userMetadata,
                Optional<OverallProgressListenerFactory>
                    overallProgressListenerFactory) {
        super(cloudStoreClient);
        this.sourceBucketName = sourceBucketName;
        this.sourceKey = sourceKey;
        this.destinationBucketName = destinationBucketName;
        this.destinationKey = destinationKey;
        this.recursive = recursive;
        this.cannedAcl = cannedAcl;
        this.s3Acl = s3Acl;
        this.storageClass = storageClass;
        this.dryRun = dryRun;
        this.ignoreAbortInjection = ignoreAbortInjection;
        this.userMetadata = userMetadata;
        this.overallProgressListenerFactory = overallProgressListenerFactory;
    }

    // for testing injection of aborts during a copy
    void injectAbort(String id)
    {
      if(!this.ignoreAbortInjection
           && (_abortCounters.decrementInjectionCounter(id) > 0))
      {
        throw new AbortInjection("forcing copy abort");
      }
    }

    static AbortCounters getAbortCounters()
    {
      return _abortCounters;
    }

    public String getSourceBucketName() {
        return sourceBucketName;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public String getDestinationBucketName() {
        return destinationBucketName;
    }

    public String getDestinationKey() {
        return destinationKey;
    }

    public Optional<String> getCannedAcl() {
        if (!cannedAcl.isPresent()) {
            if (getCloudStoreClient().getScheme().equals("s3")) {
                cannedAcl = Optional.of("bucket-owner-full-control");
            }
            else if (getCloudStoreClient().getScheme().equals("gs")) {
                cannedAcl = Optional.of("projectPrivate");
            }
        }
        return cannedAcl;
    }

    public Optional<String> getStorageClass() {
        return Optional.fromNullable(storageClass);
    }

    public Optional<AccessControlList> getS3Acl() {
        return s3Acl;
    }

    public boolean isRecursive() {
        return recursive;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public Optional<Map<String,String>> getUserMetadata() {
        return userMetadata;
    }

    public Optional<OverallProgressListenerFactory>
    getOverallProgressListenerFactory() {
        return overallProgressListenerFactory;
    }
}
