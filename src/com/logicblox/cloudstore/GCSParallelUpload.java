/*
  Copyright 2020, Infor Inc.

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
import com.google.api.services.storage.model.ComposeRequest;
import com.google.api.services.storage.model.ComposeRequest.SourceObjects.ObjectPreconditions;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Google Cloud Storage specific {@link Upload} implementation.
 */
class GCSParallelUpload
  implements Upload
{
  private ConcurrentMap<Integer, StorageObject> _uploadedParts = new ConcurrentSkipListMap<>();
  private ConcurrentLinkedQueue<String> _tempObjectNames = new ConcurrentLinkedQueue<>();
  private Storage _client;
  private Map<String, String> _meta;
  private Date _initiated;
  private ListeningExecutorService _executor;
  private UploadOptions _options;

  // for testing
  private String _uploadId;

  public GCSParallelUpload(
    UploadOptions options, Storage client, ListeningExecutorService executor,
    Map<String, String> meta, Date initiated)
  {
    _options = options;
    _client = client;
    _meta = meta;
    _initiated = initiated;
    _executor = executor;
    _uploadId = _options.getBucketName() + "/" + _options.getObjectKey();
  }

  /**
   * Uploads a part of the target object as an individual single object and, then, does checksum
   * validation for that object. GCS doesn't have the notion of S3's multi-part uploads. Instead,
   * after all individual parts have been uploaded, they are composed on the server to form the
   * final object. Composition happens in {@link GCSUpload#completeUpload}.
   */
  public ListenableFuture<Void> uploadPart(
    int partNumber, long partSize, Callable<InputStream> stream,
    OverallProgressListener progressListener)
  {
    // added to support retry testing
    _options.injectAbort(_uploadId);

    return _executor.submit(new UploadCallable(partNumber, partSize, stream, progressListener));
  }

  /**
   * Completes the upload by asking the service to compose all individual part objects.
   * Additionally, it performs checksum validation on each compose operation.
   */
  public ListenableFuture<String> completeUpload()
  {
    return (new CompleteCallable()).call();
  }

  public ListenableFuture<Void> abort()
  {
    return (new AbortCallable()).call();
  }

  public String getBucketName()
  {
    return _options.getBucketName();
  }

  public String getObjectKey()
  {
    return _options.getObjectKey();
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
    implements Callable<ListenableFuture<Void>>
  {
    public ListenableFuture<Void> call()
    {
      List<ListenableFuture<Void>> deleted = new ArrayList<>();
      for (String objectName : _tempObjectNames)
      {
        deleted.add(_executor.submit(new DeleteCallable(objectName)));
      }
      return Futures.transform(Futures.successfulAsList(deleted),
        Functions.constant(null));
    }
  }

  /**
   * GCS compose API call can merge up to 32 (either single or composite) objects. Initially, if the
   * source file has been chunked into >32 parts, then >1 compositions will take place (#chunks/32).
   * If that first round of compositions results into >32 composite objects, then more compositions
   * will follow. For huge files and/or small chunk sizes we might end up with a deep tree of such
   * compositions. {@code CompleteCallable} takes care of that by composing the appropriate number
   * of parts recursively, making sure that each level of compositions happens only after the
   * previous one has been completed.
   * <p>
   * For more details on composite objects please see
   * https://cloud.google.com/storage/docs/composite-objects.
   */
  private class CompleteCallable
    implements Callable<ListenableFuture<String>>
  {
    public ListenableFuture<String> call()
    {
      // Compose initial single (non-composite) objects
      int npass = 0;
      int nbatches = 0;
      List<ListenableFuture<StorageObject>> l = new ArrayList<>();
      List<List<StorageObject>> nonCompositesBatches = Lists.partition(
        new ArrayList<>(_uploadedParts.values()), 32);
      boolean lastCompose = nonCompositesBatches.size() == 1;
      for(List<StorageObject> nonCompositesBatch : nonCompositesBatches)
      {
        String targetObjectName;
        if (lastCompose)
        {
          targetObjectName = getObjectKey();
        }
        else
        {
          targetObjectName =
            "_" + getObjectKey() + ".cs.composite." + npass + "-" + nbatches;
          _tempObjectNames.add(targetObjectName);
        }
        l.add(composeObjects(new ArrayList<>(nonCompositesBatch), targetObjectName));
        nbatches++;
      }

      if (!lastCompose)
      {
        // Compose composite objects recursively
        while(true)
        {
          List<List<ListenableFuture<StorageObject>>> compositesBatches =
            Lists.partition(new ArrayList<>(l), 32);
          lastCompose = compositesBatches.size() == 1;
          npass++;
          nbatches = 0;
          l = new ArrayList<>();
          for(List<ListenableFuture<StorageObject>> compositesBatch : compositesBatches)
          {
            String targetObjectName;
            if (lastCompose)
            {
              targetObjectName = getObjectKey();
            }
            else
            {
              targetObjectName =
                "_" + getObjectKey() + ".cs.composite." + npass + "-" + nbatches;
              _tempObjectNames.add(targetObjectName);
            }
            l.add(composeComposites(new ArrayList<>(compositesBatch), targetObjectName));
            nbatches++;
          }
          if (lastCompose)
            break;
        }
      }

      // TODO(geoko): Assert l size == 1
      return cleanupIntermediateObjects(l.get(0));
    }

    private ListenableFuture<StorageObject> composeObjects(
      List<StorageObject> objects,
      String targetObjectName)
    {
      return _executor.submit(new ComposeCallable(objects, targetObjectName));
    }

    private ListenableFuture<StorageObject> composeComposites(
      List<ListenableFuture<StorageObject>> composites,
      String targetObjectName)
    {
      return Futures.transform(Futures.allAsList(composites), composeAsyncFunction(targetObjectName));
    }

    private AsyncFunction<List<StorageObject>, StorageObject> composeAsyncFunction(
      String targetObjectName)
    {
      return new AsyncFunction<List<StorageObject>, StorageObject>()
      {
        public ListenableFuture<StorageObject> apply(List<StorageObject> batch)
        {
          return composeObjects(batch, targetObjectName);
        }
      };
    }

    private ListenableFuture<String> cleanupIntermediateObjects(
      ListenableFuture<StorageObject> finalObject)
    {
      return Futures.transform(finalObject,
        new AsyncFunction<StorageObject, String>()
        {
          public ListenableFuture<String> apply(StorageObject object)
          {
            List<ListenableFuture<Void>> deleted = new ArrayList<>();
            for (String objectName : _tempObjectNames)
            {
              deleted.add(_executor.submit(new DeleteCallable(objectName)));
            }
            return Futures.transform(Futures.successfulAsList(deleted),
              Functions.constant(object.getEtag()));
          }
        });
    }
  }

  /**
   * The actual GCS compose API call happens here. Up to 32 (as many as the API allows) names of
   * source objects (parts) are declared in the compose request. Apart from the names, the
   * generations of the source objects are added as well to make sure only the objects uploaded as
   * part of this upload operation are composed and not unrelated objects that happen to share the
   * same names, for example due to another concurrent upload to the same object (see
   * https://cloud.google.com/storage/docs/generations-preconditions#_ParallelUpload for more
   * details).
   * <p>
   * Additionally, this class performs CRC32C checksum validation to make sure the composition
   * resulted to the expected object.
   * <p>
   * For more details on composite objects and their validation please see
   * https://cloud.google.com/storage/docs/composite-objects.
   */
  private class ComposeCallable
    implements Callable<StorageObject>
  {
    private List<StorageObject> _sourceObjects;
    private String _targetObjectName;

    public ComposeCallable(
      List<StorageObject> sourceObjects, String targetObjectName)
    {
      _sourceObjects = sourceObjects;
      _targetObjectName = targetObjectName;
    }

    public StorageObject call()
      throws IOException, BadHashException
    {
      StorageObject target = new StorageObject().setBucket(getBucketName())
        .setName(_targetObjectName)
        .setContentType("application/octet-stream")
        .setMetadata(ImmutableMap.copyOf(_meta));

      ComposeRequest request = new ComposeRequest();
      request.setDestination(target);

      long crc32_long =
        Crc32c.bytesBigEndianToLong(Base64.decodeBase64(_sourceObjects.get(0).getCrc32c()));
      StorageObject part;
      List<ComposeRequest.SourceObjects> sourceObjects = new ArrayList<>();
      for(int i = 0; i < _sourceObjects.size(); i++)
      {
        part = _sourceObjects.get(i);
        if (i > 0)
        {
          crc32_long = Crc32c.combine(crc32_long,
            Crc32c.bytesBigEndianToLong(Base64.decodeBase64(part.getCrc32c())),
            part.getSize().longValue());
        }

        ComposeRequest.SourceObjects sourceObject = new ComposeRequest.SourceObjects();
        sourceObject.setName(part.getName());
        Long generation = part.getGeneration();
        if (generation != null)
        {
          sourceObject.setGeneration(generation);
          sourceObject.setObjectPreconditions(
            new ObjectPreconditions().setIfGenerationMatch(generation));
        }
        sourceObjects.add(sourceObject);
      }
      request.setSourceObjects(sourceObjects);

      StorageObject compositeStorageObject = _client.objects()
        .compose(target.getBucket(), target.getName(), request)
        .setDestinationPredefinedAcl(_options.getCannedAcl())
        .execute();

      String remoteCrc32c = compositeStorageObject.getCrc32c();
      String localCrc32c = new String(Base64.encodeBase64(Crc32c.longToBytesBigEndian(crc32_long)));
      if(!remoteCrc32c.equals(localCrc32c))
      {
        throw new BadHashException(
          "Failed checksum validation for " + target.getBucket() + "/" + target.getName() + ". " +
            "Calculated CRC32C: " + localCrc32c + ", Expected CRC32C: " + remoteCrc32c);
      }

      return compositeStorageObject;
    }
  }

  private class DeleteCallable
    implements Callable<Void>
  {
    final private String _objectName;

    public DeleteCallable(String objectName)
    {

      _objectName = objectName;
    }

    public Void call()
      throws IOException, BadHashException
    {
      return _client.objects().delete(getBucketName(), _objectName).execute();
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
      try(Crc32cInputStream stream = new Crc32cInputStream(_streamCallable.call()))
      {
        return upload(stream);
      }
    }

    private Void upload(Crc32cInputStream stream)
      throws IOException, BadHashException
    {
      InputStreamContent mediaContent = new InputStreamContent("application/octet-stream", stream);

      // Not strictly necessary, but allows optimization in GCS
      mediaContent.setLength(_partSize);

      StorageObject objectMetadata = new StorageObject().setName("_" + getObjectKey() +
        ".cs.single." + _partNumber)
        .setMetadata(ImmutableMap.copyOf(_meta));

      Storage.Objects.Insert insertObject = _client.objects()
        .insert(getBucketName(), objectMetadata, mediaContent)
        .setPredefinedAcl(_options.getCannedAcl());
      insertObject.getMediaHttpUploader().setDisableGZipContent(true);
      if(_progressListener != null)
      {
        PartProgressEvent ppe = new PartProgressEvent(Integer.toString(_partNumber));
        MediaHttpUploaderProgressListener gcspl = new GCSUploaderProgressListener(_progressListener, ppe);
        insertObject.getMediaHttpUploader().setProgressListener(gcspl);
      }

      StorageObject res = insertObject.execute();
      _uploadedParts.put(_partNumber, res);
      _tempObjectNames.add(res.getName());

      String remoteCrc32c = res.getCrc32c();
      String localCrc32c = new String(Base64.encodeBase64(stream.getValueAsBytes()));
      String localCrc32cGuava = new String(Base64.encodeBase64(stream.getGuavaValueAsBytes()));
      if(remoteCrc32c.equals(localCrc32c))
      {
        return null;
      }
      else
      {
        throw new BadHashException(
          "Failed checksum validation for part " + (_partNumber + 1) + " of " + getBucketName() +
            "/" + getObjectKey() + ". " + "Calculated CRC32C: " + localCrc32c + ", Expected CRC32C: " +
            remoteCrc32c);
      }
    }
  }
}
