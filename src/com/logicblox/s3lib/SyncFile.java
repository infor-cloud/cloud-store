package com.logicblox.s3lib;

import java.io.File;

public class SyncFile {
  
public SyncAction _syncAction; 
private String _sourcekey;
private String _sourceBucket;
private String _destinationkey;
private String _destinationBucket;
public File _localFile; 
  
  
public SyncAction getSyncAction() {
  return _syncAction;
}


public void setSyncAction(SyncAction syncAction) {
  this._syncAction = syncAction;
}

public File getLocalFile() {
  return _localFile;
}


public void setLocalFile(File localFile) {
  this._localFile = localFile;
}

  
public String getSourcekey() {
  return _sourcekey;
}



public void setSourcKey(String sourceKey) {
  this._sourcekey = sourceKey;
}



public String getSourceBucket() {
  return _sourceBucket;
}



public void setSourceBucket(String sourceBucket) {
  this._sourceBucket = sourceBucket;
}



public String getDestinationKey() {
  return _destinationkey;
}



public void setDestinationkey(String destinationkey) {
  this._destinationkey = destinationkey;
}



public String getDestinationBucket() {
  return _destinationBucket;
}



public void setDestinationBucket(String destinationBucket) {
  this._destinationBucket = destinationBucket;
}
  
  public enum SyncAction {
    UPLOAD, DOWNLOAD, COPY, DELETE
  }
  
  public enum UrlType {
    Storage, Local
  }
  
}
