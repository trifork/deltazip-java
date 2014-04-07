package com.trifork.deltazip;

import java.io.IOException;
import java.io.OutputStream;

import java.nio.ByteBuffer;

import java.util.zip.Deflater;
import java.util.zip.Inflater;

class DeflatedMethod extends DeltaZip.CompressionMethod {
	//==================== API fulfillment ==============================
	public int methodNumber() {return DeltaZip.METHOD_DEFLATED;}

	public byte[] uncompress(ByteBuffer org, byte[] _ref_data, Inflater inflater) throws ArchiveIntegrityException {
		return DZUtil.inflate(inflater, org, org.remaining(), null);
	}

	public void compress(ByteBuffer org, byte[] _ref_data, OutputStream dst) throws IOException {
		Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION, true);
		DZUtil.deflate(deflater, org, org.remaining(), dst, null);
	}
}

