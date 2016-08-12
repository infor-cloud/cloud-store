package com.logicblox.s3lib;

public class SyncCommandOptions {
  
  private String _sourceBucket;
  private String _sourceKey;
  private String _destinationBucket;
  private String _destinationKey;
  private String _sourceFilePath;
  private String _destinationFilePath;
  
  
  SyncCommandOptions(String sourceBucket,
      String sourceKey,
      String destinationBucket,
      String destinationKey,
      String sourceFilePath,
      String destinationFilePath) {
    this._sourceBucket = sourceBucket;
    this._sourceKey = sourceKey;
    this._destinationBucket = destinationBucket;
    this._destinationKey = destinationKey;
    this._sourceFilePath = sourceFilePath;
    this._destinationFilePath = destinationFilePath;
    
  }
  
  public String getSourceBucket() {
    return _sourceBucket;
  }
  
  public String getSourceKey() {
    return _sourceKey;
  }
  
  public String getDestinationBucket() {
    return _destinationBucket;
  }
  
  public String getDestinationKey() {
    return _destinationKey;
  }
  
  public String getSourceFilePath() {
    return _sourceFilePath;
  }
  
  public String getDestinationFilePath() {
    return _destinationFilePath;
  }
  

}
