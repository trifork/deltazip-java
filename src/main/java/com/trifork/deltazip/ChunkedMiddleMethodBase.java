package com.trifork.deltazip;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.Inflater;

abstract class ChunkedMiddleMethodBase extends DeltaZip.CompressionMethod {
	private ChunkedMethod chunked_method = new ChunkedMethod();

	public byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) throws ArchiveIntegrityException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try { // For IOException; shouldn't happen
            int prefix_len = DZUtil.varlen_decode(org);
            int suffix_len = DZUtil.varlen_decode(org);

            // Add prefix:
            baos.write(ref_data, 0, prefix_len);

            // Add middle:
            byte[] ref_middle = calcRefMiddle(ref_data, prefix_len, suffix_len);
            byte[] middle = chunked_method.uncompress(org, ref_middle, inflater);
            baos.write(middle);

            // Add suffix:
            baos.write(ref_data, ref_data.length - suffix_len, suffix_len);

            baos.close();
            return baos.toByteArray();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
	}

    protected abstract byte[] calcRefMiddle(byte[] ref_data, int prefix_len, int suffix_len);

    public void compress(ByteBuffer org, byte[] ref_data, OutputStream dst) throws IOException {
		int org_pos = org.position();
		int prefix_len = longest_common_prefix(org, ref_data);
		org.position(org_pos + prefix_len);

		int suffix_len = longest_common_suffix(org, ref_data, prefix_len);
		org.limit(org.limit() - suffix_len);

		DZUtil.varlen_encode(prefix_len, dst);
		DZUtil.varlen_encode(suffix_len, dst);
		byte[] ref_middle = calcRefMiddle(ref_data, prefix_len, suffix_len);

		chunked_method.compress(org.slice(), ref_middle, dst);
	}

    protected static int longest_common_prefix(ByteBuffer a, byte[] b) {
		int limit = Math.min(a.remaining(), b.length);
		int a_start = a.position();
		int len = 0;
		for (; len < limit && a.get(a_start + len) == b[len]; len++) {}
		return len;
	}
	protected static int longest_common_suffix(ByteBuffer a, byte[] b, int b_start) {
		int limit = Math.min(a.remaining(), b.length - b_start);
		int a_end = a.limit(), b_end = b.length;
		int len = 0;
		for (; len < limit && a.get(a_end-1-len) == b[b_end-1-len]; len++) {}
		return len;
	}

	//======================================================================

}

