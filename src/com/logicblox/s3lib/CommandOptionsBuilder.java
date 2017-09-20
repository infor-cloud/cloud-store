package com.logicblox.s3lib;

public abstract class CommandOptionsBuilder
{
  CloudStoreClient _cloudStoreClient;

  public abstract CommandOptions createOptions();
}
