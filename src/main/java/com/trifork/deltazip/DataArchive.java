package com.trifork.deltazip;

import java.io.IOException;
import java.nio.ByteBuffer;

public interface DataArchive extends Iterable<ByteBuffer> {
	ByteBuffer getLatest();
	void addVersion(ByteBuffer new_version) throws IOException;
}
