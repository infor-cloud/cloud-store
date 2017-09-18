package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.StorageClass;

import java.util.Map;

/**
 * {@code CopyOptionsBuilder} is a builder for {@code CopyOptions} objects.
 * <p>
 * Setting {@code sourceBucketName}, {@code sourceObjectKey}, {@code
 * destinationBucketName} and {@code destinationObjectKey} is mandatory. All the
 * others are optional.
 */
public class CopyOptionsBuilder {
    private CloudStoreClient cloudStoreClient;
    private String sourceBucketName;
    private String sourceObjectKey;
    private String destinationBucketName;
    private String destinationObjectKey;
    private String storageClass;
    private boolean recursive = false;
    private String cannedAcl;
    private Map<String,String> userMetadata;
    private boolean dryRun = false;
    private boolean ignoreAbortInjection = false;
    private OverallProgressListenerFactory overallProgressListenerFactory;


    public CopyOptionsBuilder setCloudStoreClient(CloudStoreClient client) {
        this.cloudStoreClient = client;
        return this;
    }

    public CopyOptionsBuilder setSourceBucketName(String sourceBucketName) {
        this.sourceBucketName = sourceBucketName;
        return this;
    }

    public CopyOptionsBuilder setSourceObjectKey(String sourceObjectKey) {
        this.sourceObjectKey = sourceObjectKey;
        return this;
    }

    public CopyOptionsBuilder setDestinationBucketName(String
                                                           destinationBucketName) {
        this.destinationBucketName = destinationBucketName;
        return this;
    }

    public CopyOptionsBuilder setDestinationObjectKey(String destinationObjectKey) {
        this.destinationObjectKey = destinationObjectKey;
        return this;
    }

    public CopyOptionsBuilder setCannedACL(String cannedAcl) {
        this.cannedAcl = cannedAcl;
        return this;
    }

    public CopyOptionsBuilder setUserMetadata(Map<String,String> userMetadata) {
        this.userMetadata = userMetadata;
        return this;
    }

    public CopyOptionsBuilder setStorageClass(String storageClass) {
        this.storageClass = storageClass;
        return this;
    }
    
    public CopyOptionsBuilder setRecursive(boolean recursive) {
        this.recursive = recursive;
        return this;
    }

    public CopyOptionsBuilder setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
        return this;
    }

    public CopyOptionsBuilder setIgnoreAbortInjection(boolean ignore)
    {
      this.ignoreAbortInjection = ignore;
      return this;
    }
  

    // Disabled progress listener since AWS S3 copy progress indicator doesn't
    // notify about the copied bytes.
    //    public CopyOptionsBuilder setOverallProgressListenerFactory
    //        (OverallProgressListenerFactory overallProgressListenerFactory) {
    //        this.overallProgressListenerFactory = Optional.fromNullable
    //            (overallProgressListenerFactory);
    //        return this;
    //    }

    public CopyOptions createCopyOptions() {
        if (cloudStoreClient == null) {
            throw new UsageException("CloudStoreClient has to be set");
        }
        else if (sourceBucketName == null) {
            throw new UsageException("Source bucket name has to be set");
        }
        else if (sourceObjectKey == null) {
            throw new UsageException("Source object key has to be set");
        }
        else if (destinationBucketName == null) {
            throw new UsageException("Destination bucket name key has to be set");
        }
        else if (destinationObjectKey == null) {
            throw new UsageException("Destination object key has to be set");
        }

        if (cannedAcl != null) {
            if (!Utils.isValidCannedACLFor(
              cloudStoreClient.getStorageService(), cannedAcl)); {
                throw new UsageException("Invalid canned ACL '" + cannedAcl + "'");
            }
        }

        if (storageClass != null) {
            switch (cloudStoreClient.getStorageService())
            {
                case S3:
                    try {
                        StorageClass.fromValue(storageClass);
                    }
                    catch (IllegalArgumentException exc) {
                        throw new UsageException(
                          "Invalid storage class '" + storageClass + "'");
                    }
                    break;
                case GCS:
                    // TODO(geokollias): GCS does support something similar. Add support.
                    throw new UsageException("Storage classes are not supported " +
                                             "on GCS currently.");
                default:
                    throw new UsageException("Unknown storage service " +
                                             cloudStoreClient.getStorageService());
            }

            if (cloudStoreClient.getStorageService() == CloudStoreClient.StorageService.GCS) {
                throw new UsageException("Storage classes are not supported " +
                                         "on GCS currently.");
            }
        }

        return new CopyOptions(cloudStoreClient, sourceBucketName, sourceObjectKey,
            destinationBucketName, destinationObjectKey, cannedAcl, storageClass,
            recursive, dryRun, ignoreAbortInjection, userMetadata, overallProgressListenerFactory);
    }
}
