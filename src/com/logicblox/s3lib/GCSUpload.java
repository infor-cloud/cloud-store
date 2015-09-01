package com.logicblox.s3lib;

import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.Callable;

class GCSUpload implements Upload {
    private String md5;
    private Storage client;
    private String bucketName;
    private String key;
    private String acl;
    private Map<String, String> meta;
    private Date initiated;
    private ListeningExecutorService executor;

    public GCSUpload(Storage client,
                     String bucketName,
                     String key,
                     String acl,
                     Map<String, String> meta,
                     Date initiated,
                     ListeningExecutorService executor) {
        this.client = client;
        this.bucketName = bucketName;
        this.key = key;
        this.acl = acl;
        this.meta = meta;
        this.initiated = initiated;
        this.executor = executor;
    }

    public ListenableFuture<Void> uploadPart(int partNumber,
                                             long partSize,
                                             Callable<InputStream> stream,
                                             Optional<OverallProgressListener>
                                                 progressListener) {
        return executor.submit(new UploadCallable(partNumber, partSize, stream,
            progressListener));
    }

    public ListenableFuture<String> completeUpload() {
        return executor.submit(new CompleteCallable());
    }

    public ListenableFuture<Void> abort() {
        return executor.submit(new AbortCallable());
    }

    public String getBucket()
    {
        return bucketName;
    }

    public String getKey()
    {
        return key;
    }

    public String getId()
    {
        return null;
    }

    public Date getInitiationDate()
    {
        return initiated;
    }

    private class AbortCallable implements Callable<Void> {
        public Void call() throws Exception {
            return null;
        }
    }

    private class CompleteCallable implements Callable<String> {
        public String call() throws Exception {
            return md5;
        }
    }

    private class UploadCallable implements Callable<Void> {
        private int partNumber;
        private long partSize;
        private Callable<InputStream> streamCallable;
        private Optional<OverallProgressListener> progressListener;

        public UploadCallable(int partNumber,
                              long partSize,
                              Callable<InputStream> streamCallable,
                              Optional<OverallProgressListener> progressListener) {
            this.partNumber = partNumber;
            this.partSize = partSize;
            this.streamCallable = streamCallable;
            this.progressListener = progressListener;
        }

        public Void call() throws Exception {
            try (HashingInputStream stream = new HashingInputStream(streamCallable.call())) {
                return upload(stream);
            }
        }

        private Void upload(HashingInputStream stream) throws IOException, BadHashException {
            InputStreamContent mediaContent = new InputStreamContent(
                "application/octet-stream", stream);

            // Not strictly necessary, but allows optimization in the cloud.
            mediaContent.setLength(this.partSize);

            StorageObject objectMetadata = new StorageObject()
                .setName(key)
                .setMetadata(ImmutableMap.copyOf(meta))
                .setContentDisposition("attachment");
//                    .setAcl(ImmutableList.of(
//                            new ObjectAccessControl().setEntity("domain-example.com").setRole("READER"),
//                            new ObjectAccessControl().setEntity("user-administrator@example.com").setRole("OWNER")
//                    ));

            Storage.Objects.Insert insertObject =
                client.objects()
                    .insert(bucketName, objectMetadata, mediaContent)
                    .setPredefinedAcl(acl);

            insertObject.getMediaHttpUploader().setDisableGZipContent(true);
//              .setDisableGZipContent(true).setDirectUploadEnabled(true);

            if (progressListener.isPresent()) {
                PartProgressEvent ppe = new PartProgressEvent(Integer.toString(partNumber));
                MediaHttpUploaderProgressListener gcspl =
                    new GCSProgressListener(progressListener.get(), ppe);
                insertObject.getMediaHttpUploader()
                    .setProgressListener(gcspl);
            }

            StorageObject res = insertObject.execute();

            // GCS supports MD5 integrity check at server-sid. Since we are computing
            // MD5 on-the-fly, we can only do the check at the client-side.
            String serverMD5 = res.getMd5Hash();
            String clientMD5 = new String(Base64.encodeBase64(stream.getDigest()));
            if (serverMD5.equals(clientMD5)) {
                md5 = serverMD5;
                return null;
            } else {
                throw new BadHashException("Failed upload validation for " +
                    "'gs://" + bucketName + "/" + key + "'. " +
                    "Calculated MD5: " + clientMD5 +
                    ", Expected MD5: " + serverMD5);
            }
        }
    }
}
