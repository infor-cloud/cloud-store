package com.logicblox.s3lib;

public class DeleteOptionsBuilder extends CommandOptionsBuilder
{
  private String _bucket = null;
  private String _objectKey = null;
  private boolean _recursive = false;
  private boolean _dryRun = false;
  private boolean _forceDelete = false;
  private boolean _ignoreAbortInjection = false;

  DeleteOptionsBuilder(CloudStoreClient client)
  {
    _cloudStoreClient = client;
  }

  public DeleteOptionsBuilder setBucket(String bucket)
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

  @Override
  public DeleteOptions createOptions()
  {
    return new DeleteOptions(_cloudStoreClient, _bucket, _objectKey,
      _recursive, _dryRun, _forceDelete, _ignoreAbortInjection);
  }
  
}
