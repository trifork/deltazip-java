package com.trifork.deltazip;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

class ChunkedMiddleMethod extends ChunkedMiddleMethodBase {
	private ChunkedMethod chunked_method = new ChunkedMethod();

    public int methodNumber() {return DeltaZip.METHOD_CHUNKED_MIDDLE;}

    protected byte[] calcRefMiddle(byte[] ref_data, int prefix_len, int suffix_len) {
        return (prefix_len==0 && suffix_len==0)
                ? ref_data
                : Arrays.copyOfRange(ref_data, prefix_len, ref_data.length - suffix_len);
    }
}

