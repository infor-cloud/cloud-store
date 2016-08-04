package com.logicblox.s3lib;

public class SyncCommandOptions {
  
  private String sourcebucket;
  private String sourceoKey;
  private String destinationBucket;
  private String destinatioKey;
  private String sourceFilePath;
  private String destinationFilePath;
  
  
  SyncCommandOptions(String sourcebucket,
      String sourceoKey,
      String destinationBucket,
      String destinatioKey,
      String sourceFilePath,
      String destinationFilePath) {
    this.sourcebucket = sourcebucket;
    this.sourceoKey = sourceoKey;
    this.destinationBucket = destinationBucket;
    this.destinatioKey = destinatioKey;
    this.sourceFilePath = sourceFilePath;
    this.destinationFilePath = destinationFilePath;
    
  }
  
  public String getSourcebucket() {
    return sourcebucket;
  }
  
  public String getSourceoKey() {
    return sourceoKey;
  }
  
  public String getDestinationBucket() {
    return destinationBucket;
  }
  
  public String getDestinatioKey() {
    return destinatioKey;
  }
  
  public String getSourceFilePath() {
    return sourceFilePath;
  }
  
  public String getDestinationFilePath() {
    return destinationFilePath;
  }
  

}
