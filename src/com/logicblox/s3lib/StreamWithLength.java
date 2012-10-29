package com.logicblox.s3lib;

import java.io.InputStream;

import com.google.common.util.concurrent.ListenableFuture;

public interface StreamWithLength {
	ListenableFuture<Long> getLength();

	InputStream getStream();
}
