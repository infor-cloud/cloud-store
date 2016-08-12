package com.logicblox.s3lib;

public class SyncOptionsBuilder {
  
  private String sourcebucket;
  private String sourceKey;
  private String destinationBucket;
  private String destinationKey;
  private String sourceFilePath;
  private String destinationFilePath;
  
  public SyncOptionsBuilder setSourcebucket(String sourcebucket) {
    this.sourcebucket = sourcebucket;
    return this;
  }
  
  public SyncOptionsBuilder setSourceKey(String sourceKey) {
    this.sourceKey = sourceKey;
    return this;
  }
  
  public SyncOptionsBuilder setDestinationBucket(String destinationBucket) {
    this.destinationBucket = destinationBucket;
    return this;
  }
  
  public SyncOptionsBuilder setDestinationKey(String destinationKey) {
    this.destinationKey = destinationKey;
    return this;
  }
  
  public SyncOptionsBuilder setSourceFilePath(String sourceFilePath) {
    this.sourceFilePath = sourceFilePath;
    return this;
  }
  
  public SyncOptionsBuilder setDestinationFilePath(String destinationFilePath) {
    this.destinationFilePath = destinationFilePath;
    return this;
  }
  
  public SyncCommandOptions createSyncCommandOptions() {
    return new  SyncCommandOptions( sourcebucket, sourceKey, destinationBucket, destinationKey, sourceFilePath, destinationFilePath) ;
    
  }
  
}
