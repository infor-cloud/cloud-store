package com.logicblox.s3lib;

import java.io.File;

public class SyncFile {
  
public SyncAction syncAction; 
private String _source_key;
private String _source_bucket;
private String _destination_key;
private String _destination_bucket;
public File localFile; 
  
  
public SyncAction getSyncAction() {
  return syncAction;
}


public void setSyncAction(SyncAction syncAction) {
  this.syncAction = syncAction;
}

public File getLocalFile() {
  return localFile;
}


public void setLocalFile(File localFile) {
  this.localFile = localFile;
}

  
public String get_source_key() {
  return _source_key;
}



public void set_source_key(String _source_key) {
  this._source_key = _source_key;
}



public String get_source_bucket() {
  return _source_bucket;
}



public void set_source_bucket(String _source_bucket) {
  this._source_bucket = _source_bucket;
}



public String get_destination_key() {
  return _destination_key;
}



public void set_destination_key(String _destination_key) {
  this._destination_key = _destination_key;
}



public String get_destination_bucket() {
  return _destination_bucket;
}



public void set_destination_bucket(String _destination_bucket) {
  this._destination_bucket = _destination_bucket;
}

  public enum SyncAction {
    UPLOAD ,
  DOWNLOAD ,
  COPY,
  DELETELOCAL,
  DELETEREMOTE;
  }
  
  
}
