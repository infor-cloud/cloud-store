package com.logicblox.s3lib;

import com.google.common.base.Optional;
import java.util.HashMap;
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
public class CopyOptions {
    private final String sourceBucketName;
    private final String sourceKey;
    private final String destinationBucketName;
    private final String destinationKey;
    private final boolean recursive;
    private final boolean dryRun;
    private final boolean ignoreAbortInjection;
    // TODO(geo): Revise use of Optionals. E.g. it's not a good idea to use them
    // as fields.
    private final Optional<String> cannedAcl;
    private final String storageClass;
    private final Optional<OverallProgressListenerFactory>
        overallProgressListenerFactory;

    // for testing injecion of aborts during a copy
    private static int _abortInjectionCounter = 0;
    private static boolean _globalAbortCounter = false;
    private static Object _abortSync = new Object();
    private static Map<String,Integer> _injectionCounters = new HashMap<String,Integer>();


    CopyOptions(String sourceBucketName,
                String sourceKey,
                String destinationBucketName,
                String destinationKey,
                Optional<String> cannedAcl,
                String storageClass,
                boolean recursive,
                boolean dryRun,
                boolean ignoreAbortInjection,
                Optional<OverallProgressListenerFactory>
                    overallProgressListenerFactory) {
        this.sourceBucketName = sourceBucketName;
        this.sourceKey = sourceKey;
        this.destinationBucketName = destinationBucketName;
        this.destinationKey = destinationKey;
        this.recursive = recursive;
        this.cannedAcl = cannedAcl;
        this.storageClass = storageClass;
        this.dryRun = dryRun;
        this.ignoreAbortInjection = ignoreAbortInjection;
        this.overallProgressListenerFactory = overallProgressListenerFactory;
    }

    // for testing injection of aborts during a copy
    static void setAbortInjectionCounter(int counter)
    {
      synchronized(_abortSync)
      {
        _abortInjectionCounter = counter;
      }
    }

    // for testing injection of aborts during a copy
    static int decrementAbortInjectionCounter(String id)
    {
      synchronized(_abortSync)
      {
        if(_abortInjectionCounter <= 0)
          return 0;

        if(_globalAbortCounter)
          id = "";

        if(!_injectionCounters.containsKey(id))
          _injectionCounters.put(id, _abortInjectionCounter);
        int current = _injectionCounters.get(id);
        _injectionCounters.put(id, current - 1);
        return current;
      }
    }


    static void clearAbortInjectionCounters()
    {
      synchronized(_abortSync)
      {
        _injectionCounters.clear();
      }
    }


    // if true, use a single abort counter for all delete operations.
    // otherwise (default), use a separate counter for each delete
    static boolean useGlobalAbortCounter(boolean b)
    {
      synchronized(_abortSync)
      {
        boolean old = _globalAbortCounter;
        _globalAbortCounter = b;
        return old;
      }
    }

    
    public boolean ignoreAbortInjection()
    {
      return this.ignoreAbortInjection;
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
        return cannedAcl;
    }

    public Optional<String> getStorageClass() {
        return Optional.fromNullable(storageClass);
    }

    public boolean isRecursive() {
        return recursive;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public Optional<OverallProgressListenerFactory>
    getOverallProgressListenerFactory() {
        return overallProgressListenerFactory;
    }
}
