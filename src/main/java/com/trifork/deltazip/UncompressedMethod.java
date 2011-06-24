package com.trifork.deltazip;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.WritableByteChannel;
import java.nio.channels.Channels;

import java.util.zip.Inflater;

class UncompressedMethod extends DeltaZip.CompressionMethod {
	public int methodNumber() {return DeltaZip.METHOD_UNCOMPRESSED;}

	public void compress(ByteBuffer org, byte[] ref_data, OutputStream dst) {
		try {
			WritableByteChannel channel = Channels.newChannel(dst);
			channel.write(org);
		} catch (IOException ioe) {throw new RuntimeException(ioe);}
	}

	public byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) {
		return DZUtil.remainingToByteArray(org);
	}
}

