package com.logicblox.s3lib;

import java.io.InputStream;

public interface StreamWithLength {
	long getLength();

	InputStream getStream();
}
