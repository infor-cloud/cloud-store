package com.logicblox.s3lib;

public class CommandOptions
{
  private final CloudStoreClient _cloudStoreClient;

  CommandOptions(CloudStoreClient cloudStoreClient)
  {
    _cloudStoreClient = cloudStoreClient;
  }

  public CloudStoreClient getCloudStoreClient()
  {
    return _cloudStoreClient;
  }
}
