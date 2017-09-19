package com.logicblox.s3lib;

public class DeleteOptionsBuilder
{
  private CloudStoreClient _cloudStoreClient;
  private String _bucket = null;
  private String _objectKey = null;
  private boolean _recursive = false;
  private boolean _dryRun = false;
  private boolean _forceDelete = false;
  private boolean _ignoreAbortInjection = false;


  public DeleteOptionsBuilder setCloudStoreClient(CloudStoreClient client)
  {
    _cloudStoreClient = client;
    return this;
  }

  public DeleteOptionsBuilder setBucketName(String bucket)
  {
    _bucket = bucket;
    return this;
  }
  
  public DeleteOptionsBuilder setObjectKey(String objectKey)
  {
    _objectKey = objectKey;
    return this;
  }
  
  public DeleteOptionsBuilder setRecursive(boolean recursive)
  {
    _recursive = recursive;
    return this;
  }
  
  public DeleteOptionsBuilder setDryRun(boolean dryRun)
  {
    _dryRun = dryRun;
    return this;
  }
  
  public DeleteOptionsBuilder setForceDelete(boolean force)
  {
    _forceDelete = force;
    return this;
  }
  
  public DeleteOptionsBuilder setIgnoreAbortInjection(boolean ignore)
  {
    _ignoreAbortInjection = ignore;
    return this;
  }

  private void validateOptions()
  {
    if (_cloudStoreClient == null) {
      throw new UsageException("CloudStoreClient has to be set");
    }
    else if (_bucket == null) {
      throw new UsageException("Bucket has to be set");
    }
    else if (_objectKey == null) {
      throw new UsageException("Object key has to be set");
    }
  }

  public DeleteOptions createDeleteOptions()
  {
    validateOptions();

    return new DeleteOptions(_cloudStoreClient, _bucket, _objectKey,
      _recursive, _dryRun, _forceDelete, _ignoreAbortInjection);
  }
}
