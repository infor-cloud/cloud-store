package com.logicblox.s3lib;

import java.io.InputStream;

import com.google.common.util.concurrent.ListenableFuture;

public interface Upload {
	ListenableFuture<Void> uploadPart(int partNumber, InputStream stream, long partSize);

	ListenableFuture<String> completeUpload();

	ListenableFuture<Void> abort();
}