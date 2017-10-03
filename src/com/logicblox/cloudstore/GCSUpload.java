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

import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;

class GCSUpload
  implements Upload
{
  private String _md5;
  private Storage _client;
  private String _bucketName;
  private String _objectKey;
  private Map<String, String> _meta;
  private Date _initiated;
  private ListeningExecutorService _executor;
  private UploadOptions _options;

  // for testing
  private String _uploadId;

  public GCSUpload(
    Storage client, String bucketName, String objectKey, Map<String, String> meta, Date initiated,
    ListeningExecutorService executor, UploadOptions options)
  {
    _client = client;
    _bucketName = bucketName;
    _objectKey = objectKey;
    _meta = meta;
    _initiated = initiated;
    _executor = executor;
    _uploadId = bucketName + "/" + objectKey;
    _options = options;
  }

  public ListenableFuture<Void> uploadPart(
    int partNumber, long partSize, Callable<InputStream> stream,
    OverallProgressListener progressListener)
  {
    // added to support retry testing
    _options.injectAbort(_uploadId);

    return _executor.submit(new UploadCallable(partNumber, partSize, stream, progressListener));
  }

  public ListenableFuture<String> completeUpload()
  {
    return _executor.submit(new CompleteCallable());
  }

  public ListenableFuture<Void> abort()
  {
    return _executor.submit(new AbortCallable());
  }

  public String getBucketName()
  {
    return _bucketName;
  }

  public String getObjectKey()
  {
    return _objectKey;
  }

  public String getId()
  {
    return null;
  }

  public Date getInitiationDate()
  {
    return _initiated;
  }

  private class AbortCallable
    implements Callable<Void>
  {
    public Void call()
      throws Exception
    {
      return null;
    }
  }

  private class CompleteCallable
    implements Callable<String>
  {
    public String call()
      throws Exception
    {
      return _md5;
    }
  }

  private class UploadCallable
    implements Callable<Void>
  {
    private int _partNumber;
    private long _partSize;
    private Callable<InputStream> _streamCallable;
    private OverallProgressListener _progressListener;

    public UploadCallable(
      int partNumber, long partSize, Callable<InputStream> streamCallable,
      OverallProgressListener progressListener)
    {
      _partNumber = partNumber;
      _partSize = partSize;
      _streamCallable = streamCallable;
      _progressListener = progressListener;
    }

    public Void call()
      throws Exception
    {
      try(HashingInputStream stream = new HashingInputStream(_streamCallable.call()))
      {
        return upload(stream);
      }
    }

    private Void upload(HashingInputStream stream)
      throws IOException, BadHashException
    {
      InputStreamContent mediaContent = new InputStreamContent("application/octet-stream", stream);

      // Not strictly necessary, but allows optimization in the cloud.
      mediaContent.setLength(_partSize);

      StorageObject objectMetadata = new StorageObject().setName(_objectKey)
        .setMetadata(ImmutableMap.copyOf(_meta));

      Storage.Objects.Insert insertObject = _client.objects()
        .insert(_bucketName, objectMetadata, mediaContent);

      insertObject.setPredefinedAcl(_options.getCannedAcl());
      insertObject.getMediaHttpUploader().setDisableGZipContent(true);
      //              .setDisableGZipContent(true).setDirectUploadEnabled(true);

      if(_progressListener != null)
      {
        PartProgressEvent ppe = new PartProgressEvent(Integer.toString(_partNumber));
        MediaHttpUploaderProgressListener gcspl = new GCSProgressListener(_progressListener, ppe);
        insertObject.getMediaHttpUploader().setProgressListener(gcspl);
      }

      StorageObject res = insertObject.execute();

      // GCS supports MD5 integrity check at server-sid. Since we are computing
      // MD5 on-the-fly, we can only do the check at the client-side.
      String serverMD5 = res.getMd5Hash();
      String clientMD5 = new String(Base64.encodeBase64(stream.getDigest()));
      if(serverMD5.equals(clientMD5))
      {
        _md5 = serverMD5;
        return null;
      }
      else
      {
        throw new BadHashException(
          "Failed upload validation for " + "'gs://" + _bucketName + "/" + _objectKey + "'. " +
            "Calculated MD5: " + clientMD5 + ", Expected MD5: " + serverMD5);
      }
    }
  }
}
