package com.logicblox.s3lib;

import com.amazonaws.services.s3.model.StorageClass;

public class S3StorageClassHandler implements StorageClassHandler
{
  @Override
  public boolean isStorageClassValid(String storageClass)
  {
    try
    {
      StorageClass.fromValue(storageClass);
    }
    catch (IllegalArgumentException exc)
    {
      return false;
    }
    return true;
  }
}
