package com.logicblox.s3lib;

public interface RandomAccessStream {
	StreamWithLength getStream(long start);

	StreamWithLength getStream(long start, long end);
}
