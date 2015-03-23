package com.logicblox.s3lib;

import com.google.api.client.googleapis.media.MediaHttpUploader;
import com.google.api.client.googleapis.media.MediaHttpUploaderProgressListener;

class GCSProgressListener
    implements MediaHttpUploaderProgressListener {
    final private OverallProgressListener opl;
    final private PartProgressEvent ppe;

    public GCSProgressListener(OverallProgressListener opl,
                               PartProgressEvent ppe) {
        this.opl = opl;
        this.ppe = ppe;
    }

    @Override
    public void progressChanged(MediaHttpUploader uploader) {
        switch (uploader.getUploadState()) {
            case MEDIA_IN_PROGRESS:
                // TODO: Progress works iff you have a content length specified.
                ppe.setTransferredBytes(uploader.getNumBytesUploaded());
                opl.progress(ppe);
                break;
            case MEDIA_COMPLETE:
                ppe.setTransferredBytes(uploader.getNumBytesUploaded());
                opl.progress(ppe);
                break;
            default:
                break;
        }
    }
}