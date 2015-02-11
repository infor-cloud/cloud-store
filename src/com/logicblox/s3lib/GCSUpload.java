package com.logicblox.s3lib;

import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.commons.codec.binary.Base64;

import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Callable;

class GCSUpload implements Upload {
    private String md5;
    private Storage client;
    private String bucketName;
    private String key;
    private String acl;
    private Map<String, String> meta;
    private ListeningExecutorService executor;
    private boolean progress;

    public GCSUpload(Storage client, String bucketName, String key, String acl, Map<String, String> meta,
                     ListeningExecutorService executor, boolean progress) {
        this.client = client;
        this.bucketName = bucketName;
        this.key = key;
        this.acl = acl;
        this.meta = meta;
        this.executor = executor;
        this.progress = progress;
    }

    public ListenableFuture<Void> uploadPart(int partNumber, InputStream stream, long partSize) {
        return executor.submit(new UploadCallable(partNumber, stream, partSize));
    }

    public ListenableFuture<String> completeUpload() {
        return executor.submit(new CompleteCallable());
    }

    public ListenableFuture<Void> abort() {
        return executor.submit(new AbortCallable());
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
        private HashingInputStream stream;
        private long partSize;

        public UploadCallable(int partNumber, InputStream stream, long partSize) {
            this.partNumber = partNumber;
            this.partSize = partSize;
            this.stream = new HashingInputStream(stream);
        }

        public Void call() throws Exception {
            InputStreamContent mediaContent = new InputStreamContent(
                    "application/octet-stream", this.stream);

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
                    client.objects().insert(bucketName, objectMetadata, mediaContent)
                            .setPredefinedAcl(acl);

            insertObject.getMediaHttpUploader().setDisableGZipContent(true);
//              .setDisableGZipContent(true).setDirectUploadEnabled(true);

            if (progress) {
                insertObject.getMediaHttpUploader().setProgressListener(
                        new GCSUploadProgressListener(
                                key + " part " + (partNumber + 1),
                                partSize / 10,
                                partSize));
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
              throw new BadHashException("Failed upload checksum validation for " +
                  bucketName + "/" + key + ". " +
                  "Calculated MD5: " + clientMD5 +
                  ", Expected MD5: " + serverMD5);
            }
        }
    }

    private static class GCSUploadProgressListener
            extends ProgressListener
            implements MediaHttpUploaderProgressListener {

        public GCSUploadProgressListener(String name, long intervalInBytes, long totalSizeInBytes) {
            super(name, "upload", intervalInBytes, totalSizeInBytes);
        }

        @Override
        public void progressChanged(MediaHttpUploader uploader) {
            switch (uploader.getUploadState()) {
                case INITIATION_STARTED:
                    started();
                    break;
                case MEDIA_IN_PROGRESS:
                    // TODO: Progress works iff you have a content length specified.
                    // progressCum(uploader.getProgress());
                    progressCum(uploader.getNumBytesUploaded());
                    break;
                case MEDIA_COMPLETE:
                    progressCum(uploader.getNumBytesUploaded());
                    complete();
                    break;
                default:
                    break;
            }
        }
    }
}
