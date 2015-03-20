package com.logicblox.s3lib;

class S3ProgressListener
    implements com.amazonaws.event.ProgressListener {
    final private OverallProgressListener opl;
    final private PartProgressEvent ppe;

    public S3ProgressListener(OverallProgressListener opl,
                              PartProgressEvent ppe) {
        this.opl = opl;
        this.ppe = ppe;
    }

    @Override
    public void progressChanged(com.amazonaws.event.ProgressEvent event) {
        ppe.setLastTransferBytes(event.getBytesTransferred());
        opl.progress(ppe);
    }
}
