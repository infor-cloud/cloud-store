package com.logicblox.s3lib;

public class SyncOptionsBuilder {
  
  private String sourcebucket;
  private String sourceoKey;
  private String destinationBucket;
  private String destinatioKey;
  private String sourceFilePath;
  private String destinationFilePath;
  
  public SyncOptionsBuilder setSourcebucket(String sourcebucket) {
    this.sourcebucket = sourcebucket;
    return this;
  }
  
  public SyncOptionsBuilder setSourceKey(String sourceoKey) {
    this.sourceoKey = sourceoKey;
    return this;
  }
  
  public SyncOptionsBuilder setDestinationBucket(String destinationBucket) {
    this.destinationBucket = destinationBucket;
    return this;
  }
  
  public SyncOptionsBuilder setDestinationKey(String destinatioKey) {
    this.destinatioKey = destinatioKey;
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
    return new  SyncCommandOptions( sourcebucket, sourceoKey, destinationBucket, destinatioKey, sourceFilePath, destinationFilePath) ;
    
  }
  
}
