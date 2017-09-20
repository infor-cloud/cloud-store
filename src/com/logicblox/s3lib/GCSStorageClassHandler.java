package com.logicblox.s3lib;

public class GCSStorageClassHandler implements StorageClassHandler
{
  @Override
  public boolean isStorageClassValid(String storageClass)
  {
    // TODO: GCS does support something similar. Add support.
    throw new UsageException("Storage classes are not supported " +
                             "on GCS currently.");
  }
}
