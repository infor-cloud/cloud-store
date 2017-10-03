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

package com.logicblox.cloudstore;

import java.io.File;
import java.util.Date;
import java.util.Optional;

/**
 * Represents a file that has been uploaded to or downloaded from a cloud store service.
 * This object can contain a file's checksum, key, bucket, version, size, and modification
 * timestamp.  For uploads or downloads, it can also contain a reference to a local File
 * object.
 */
public class StoreFile
{

  private File _localFile;
  private String _eTag;
  private String _key;
  private String _bucket;
  private Optional<String> _versionId = Optional.empty();
  private Optional<Long> _size = Optional.empty();
  private Optional<Date> _timestamp = Optional.empty();

  /**
   * Create a new StoreFile with no properties set.
   */
  public StoreFile()
  {
  }

  /**
   * Create a StoreFile for a particular file in a bucket.
   *
   * @param bucket Name of the bucket containing the file
   * @param key Key that identifies the file in the store
   */
  public StoreFile(String bucket, String key)
  {
    _bucket = bucket;
    _key = key;
  }

  /**
   * Return the local File object for an upload or download operation.
   *
   * @return local file object
   */
  public File getLocalFile()
  {
    return _localFile;
  }

  /**
   * Set the local File for an upload or download operation.
   * 
   * @param f local file object
   */
  public void setLocalFile(File f)
  {
    _localFile = f;
  }

  /**
   * Return the ETag (checksum) associated with a file in a cloud store service.
   *
   * @return file checksum
   */
  public String getETag()
  {
    return _eTag;
  }

  /**
   * Set the ETag (checksum) associated with a file in a cloud store service.
   *
   * @param tag file checksum
   */
  public void setETag(String tag)
  {
    _eTag = tag;
  }

  /**
   * Return the key of the file represented by this StoreFile.
   *
   * @return file key
   */
  public String getObjectKey()
  {
    return _key;
  }

  /**
   * Set the key of the file represented by this StoreFile.
   *
   * @param key file key
   */
  public void setObjectKey(String key)
  {
    _key = key;
  }

  /**
   * Return the name of the bucket containing a file represented by this StoreFile.
   *
   * @return bucket name
   */
  public String getBucketName()
  {
    return _bucket;
  }

  /**
   * Set the name of the bucket containing a file represented by this StoreFile.
   * 
   * @param bucket name of bucket
   */
  public void setBucketName(String bucket)
  {
    _bucket = bucket;
  }

  /**
   * Return the version ID of a file in a cloud store service.
   *
   * @return version ID for a file
   */
  public Optional<String> getVersionId()
  {
    return _versionId;
  }

  /**
   * Set the version ID of a file in a cloud store service.
   *
   * @param versionId version ID for a file
   */
  public void setVersionId(String versionId)
    throws IllegalArgumentException
  {
    if(versionId == null)
    {
      throw new IllegalArgumentException("Error : Version Id should not be set to Null");
    }
    _versionId = Optional.of(versionId);
  }

  /**
   * Return the size of a file in a cloud store service.
   *
   * @return size of file
   */
  public Optional<Long> getSize()
  {
    return _size;
  }

  /**
   * Set the size of a file in a cloud store service.
   *
   * @param size size of file
   */
  public void setSize(Long size)
    throws IllegalArgumentException
  {
    if(size == null)
    {
      throw new IllegalArgumentException("Error : size should not be set to Null");
    }
    _size = Optional.of(size);

  }

  /**
   * Return the modification time for a file in a cloud store service.
   *
   * @return modification time for file
   */
  public Optional<Date> getTimestamp()
  {
    return _timestamp;
  }

  /**
   * Set the modification time for a file in a cloud store service.
   *
   * @param timestamp modification time for file
   */
  public void setTimestamp(Date timestamp)
    throws IllegalArgumentException
  {
    if(timestamp == null)
    {
      throw new IllegalArgumentException("Error : timestamp should not be set to Null");
    }
    _timestamp = Optional.of(timestamp);
  }

}
