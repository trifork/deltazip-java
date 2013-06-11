package com.trifork.deltazip;

import java.util.Arrays;

class ChunkedMiddle2Method extends ChunkedMiddleMethodBase {
	private ChunkedMethod chunked_method = new ChunkedMethod();

    public int methodNumber() {return DeltaZip.METHOD_CHUNKED_MIDDLE2;}

    protected byte[] calcRefMiddle(byte[] ref_data, int prefix_len, int suffix_len) {
        int cut_len = Math.max(0, prefix_len - ChunkedMethod.CHUNK_SIZE);
        return (cut_len==0)
                ? ref_data
                : Arrays.copyOfRange(ref_data, cut_len, ref_data.length);
    }
}

