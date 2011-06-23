package com.trifork.deltazip;

import java.io.IOException;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

import java.nio.ByteBuffer;

import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

class ChunkedMiddleMethod extends DeltaZip.CompressionMethod {
	private ChunkedMethod chunked_method = new ChunkedMethod();

	public int methodNumber() {return DeltaZip.METHOD_CHUNKED_MIDDLE;}

	public byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int prefix_len = varlen_decode(org);
		int suffix_len = varlen_decode(org);

		// Add prefix:
		baos.write(ref_data, 0, prefix_len);

		// Add middle:
		int ref_middle_end = ref_data.length - suffix_len;
		byte[] ref_middle = Arrays.copyOfRange(ref_data, prefix_len, ref_middle_end);
		byte[] middle = chunked_method.uncompress(org, ref_middle, inflater);
		baos.write(middle);

		// Add suffix:
		baos.write(ref_data, ref_data.length - suffix_len, suffix_len);

		baos.close();
		return baos.toByteArray();
	}

	public void compress(ByteBuffer org, byte[] ref_data, OutputStream dst) throws IOException {
		int org_pos = org.position();
		int prefix_len = longest_common_prefix(org, ref_data);
		org.position(org_pos + prefix_len);

		int suffix_len = longest_common_suffix(org, ref_data, prefix_len);
		org.limit(org.limit() - suffix_len);

		varlen_encode(prefix_len, dst);
		varlen_encode(suffix_len, dst);
		byte[] ref_middle = (prefix_len==0 && suffix_len==0)
			? ref_data
			: Arrays.copyOfRange(ref_data, prefix_len, ref_data.length - suffix_len);
		
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

	public static void varlen_encode(int value, OutputStream out) throws IOException{
		int shift = 0;
// 		System.err.print("Encoding "+value+" as ");
		while ((value >>> shift) >= 0x80) shift += 7;
		for (; shift>=0; shift -= 7) {
			byte b = (byte)((value >>> shift) & 0x7F);
			if (shift>0) b |= 0x80;
// 			System.err.print(" "+(b & 0xFF));
			out.write(b);
		}
// 		System.err.println();
	}

	public static int varlen_decode(ByteBuffer in) throws IOException {
		long acc = 0;
		int bits = 0;
		boolean more;
// 		System.err.print("Decoding");
		do {
			int b = in.get();
// 			System.err.print(" "+(b & 0xff));
			more = (b < 0);
			b &= 0x7F;
			acc = (acc << 7) | (b & 0x7F);
			bits += 7;
			if (acc > Integer.MAX_VALUE) throw new IOException("Variable-length encoded integer is too large: "+acc);
		} while (more);
// 		System.err.println(" as "+acc);
		return (int) acc;
	}
}

