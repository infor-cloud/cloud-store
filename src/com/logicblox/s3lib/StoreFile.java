/*
  Copyright 2017, Infor Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package com.logicblox.s3lib;

import java.io.File;
import java.util.Optional;
import java.util.Date;

public class StoreFile {
  
  private File _localFile;
  private String _eTag;
  private String _key;
  private String _bucket;
  private Optional<String> _versionId = Optional.empty();
  private Optional<Long> _size = Optional.empty();
  private Optional<Date> _timestamp = Optional.empty();

  public StoreFile()
  {
  }
  
  public StoreFile(String bucket, String key)
  {
    _bucket = bucket;
    _key = key;
  }
  
  public File getLocalFile() {
    return _localFile;
  }
  
  public void setLocalFile(File f) {
    _localFile = f;
  }
  
  public String getETag() {
    return _eTag;
  }
  
  public void setETag(String tag) {
    _eTag = tag;
  }
  
  public String getKey() {
    return _key;
  }
  
  public void setKey(String key) {
    _key = key;
  }
  
  public String getBucketName() {
    return _bucket;
  }
  
  public void setBucketName(String bucket) {
    _bucket = bucket;
  }
  
  public Optional<String> getVersionId() {
    return _versionId;
  }
  
  public void setVersionId(String versionId) throws IllegalArgumentException {
    if (versionId == null) {
      throw new IllegalArgumentException("Error : Version Id should not be set to Null");
    }
    _versionId = Optional.of(versionId);
  }
  
  public Optional<Long> getSize() {
    return _size;
  }
  
  public void setSize(Long size) throws IllegalArgumentException {
    if (size == null) {
      throw new IllegalArgumentException("Error : size should not be set to Null");
    }
    _size = Optional.of(size);
    
  }
  
  public Optional<Date> getTimestamp() {
    return _timestamp;
  }
  
  public void setTimestamp(Date timestamp) throws IllegalArgumentException {
    if (timestamp == null) {
      throw new IllegalArgumentException("Error : timestamp should not be set to Null");
    }
    _timestamp = Optional.of(timestamp);
  }

}
