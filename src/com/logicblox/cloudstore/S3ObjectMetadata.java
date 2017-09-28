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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.util.HashMap;
import java.util.Map;

class S3ObjectMetadata
{
  private AmazonS3 _client;
  private ListeningExecutorService _executor;
  private ObjectMetadata _meta;
  private String _objectKey;
  private String _bucketName;
  private String _version;

  public S3ObjectMetadata(
    AmazonS3 client, String objectKey, String bucketName, String version, ObjectMetadata meta,
    ListeningExecutorService executor)
  {
    _client = client;
    _objectKey = objectKey;
    _bucketName = bucketName;
    _version = version;
    _executor = executor;
    _meta = meta;
  }

  public String getBucket()
  {
    return _bucketName;
  }

  public String getObjectKey()
  {
    return _objectKey;
  }

  public ObjectMetadata getAllMetadata()
  {
    return _meta;
  }

  // make sure all metadata keys are lowercase.  some servers (minio) return
  // mixed case keys
  public Map<String, String> getUserMetadata()
  {
    Map<String, String> userData = _meta.getUserMetadata();
    Map<String, String> fixed = new HashMap<String, String>();
    for(Map.Entry<String, String> e : userData.entrySet())
      fixed.put(e.getKey().toLowerCase(), e.getValue());
    return fixed;
  }

  public String getETag()
  {
    return _meta.getETag();
  }

  public long getLength()
  {
    return _meta.getContentLength();
  }

  public String getVersion()
  {
    return _version;
  }
}
