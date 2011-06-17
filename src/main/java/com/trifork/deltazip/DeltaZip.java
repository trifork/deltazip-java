package com.trifork.deltazip;

import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.Channels;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.IOException;

public class DeltaZip {

	//==================== Constants =======================================
	/** For some reason, zlib can only use 32K-262 bytes (=#7EFA) of a dictionary.
	 * (See http://www.zlib.net/manual.html)
	 * So we use a slightly smaller window size: */
	private static final int WINDOW_SIZE = 0x7E00;
	private static final int CHUNK_SIZE  = WINDOW_SIZE / 2;

	/** For when we need to ensure the deflated ouput will fit in 64KB:
	 * (From http://www.zlib.net/zlib_tech.html:
	 *  "...an overhead of five bytes per 16 KB block (about 0.03%), plus a one-time overhead of
	 *   six bytes for the entire stream")
	 */
	private static final int LIMIT_SO_DEFLATED_FITS_IN_64KB = 65000;

	private static final int METHOD_UNCOMPRESSED = 0;
	private static final int METHOD_CHUNKED_DEFLATE = 2;

	protected static final CompressionMethod[] COMPRESSION_METHODS;
	static {
		COMPRESSION_METHODS = new CompressionMethod[16];
		COMPRESSION_METHODS[METHOD_UNCOMPRESSED] = new UncompressedMethod();
		COMPRESSION_METHODS[METHOD_CHUNKED_DEFLATE] = new ChunkedDeflateMethod();
	}
	

	//==================== Fields ==========================================

	private final Inflater inflater = new Inflater(true);
	private final Access access;

	private long       current_pos, current_size;
	private int        current_method;
	private byte[]     current_version;
	private ByteBuffer exposed_current_version;
	
	public DeltaZip(Access access) throws IOException {
		this.access = access;
		set_initial_position();
		if (hasPrevious()) previous();
	}

	/** Get the revision pointed to by the cursor. */
	public ByteBuffer get() {
		return exposed_current_version;
	}

	/** Tells whether there are older revisions. */
	public boolean hasPrevious() {
		return current_pos > 0;
	}

	/** Retreat the cursor.
	 *  @throws InvalidStateException if the cursor is pointing at the first revision.
	 */
	public void previous() throws IOException {
		if (!hasPrevious()) throw new IllegalStateException();
		goto_previous_position_and_compute_current_version();
	}

	//==================== Internals =======================================

	protected void set_initial_position() {
		current_pos = access.getSize();
	}

	protected void goto_previous_position_and_compute_current_version() throws IOException {
		ByteBuffer tag_buf = access.pread(current_pos-4, 4);
		int tag = tag_buf.getInt(0);
		int size = tag &~ (-1 << 28);
		int method = (tag >> 28) & 15;
// 		System.err.println("DB| tag="+tag+" -> "+method+":"+size);

		// Get start-tag plus data:
		long start_pos = current_pos-size-8;
		ByteBuffer data_buf = access.pread(start_pos, size+4);
		data_buf.rewind();
		int start_tag = data_buf.getInt();
		if (start_tag != tag) throw new IOException("Data error - tag mismatch @ "+start_pos+";"+current_pos);

		current_pos = start_pos;
		compute_current_version(method, data_buf);
	}

	protected void compute_current_version(int method, ByteBuffer data_buf) throws IOException {
		CompressionMethod cm = COMPRESSION_METHODS[method];
		if (cm==null) throw new IOException("Invalid compression method: "+method+" @ "+current_pos);

		current_method = method;
		current_version = cm.uncompress(data_buf, current_version, inflater);
		exposed_current_version = ByteBuffer.wrap(current_version).asReadOnlyBuffer();
	}

	//==================== Compression methods =============================
	protected static abstract class CompressionMethod {
		public abstract ByteBuffer compress(ByteBuffer org, byte[] ref_data) throws IOException;
		public abstract byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) throws IOException;
	}

	protected static class UncompressedMethod extends CompressionMethod {
		public ByteBuffer compress(ByteBuffer org, byte[] ref_data) {return org;}
		public byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) {return toByteArray(org);}
	}

	protected static class ChunkedDeflateMethod extends CompressionMethod {
		public ByteBuffer compress(ByteBuffer org, byte[] ref_data) throws IOException {
			return null; //TODO
		}

		public byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) throws IOException {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			int ref_data_offset = 0;
			while (org.hasRemaining()) {
				System.err.println("DB| uncompress: remaining="+org.remaining());
				// Parse chunk header:
				int meth = org.get();
				int comp_data_size = org.getChar(); // unsigned short
				int rskip_spec = meth & 7;
				if ((meth &~ 7) != 0) throw new IOException("Invalid chunk encoding: "+meth);

				// Determine and set dictionary:
				int rskip = spec_to_rskip(rskip_spec);
				ref_data_offset += rskip;
				int dict_size = Math.min(WINDOW_SIZE, ref_data.length-ref_data_offset);
				//inflater.setDictionary(ref_data, ref_data_offset, dict_size); // Too early when headers are on...
				Dictionary dict = new Dictionary(ref_data, ref_data_offset, dict_size);

				// Inflate:
				int before = baos.size();
				inflate(inflater, org, comp_data_size, baos, dict);
				int after = baos.size();
				System.err.println("DB| inflated "+comp_data_size+" to "+(after-before));
			}
			return baos.toByteArray();
		}

		public static int spec_to_rskip(int rskip_spec) {
			return rskip_spec * (CHUNK_SIZE / 2);
		}

		protected static void inflate(Inflater inflater, ByteBuffer src, int comp_length, OutputStream dst, Dictionary dict) throws IOException {
			InflaterOutputStream ios = new InflaterOutputStream(dst, inflater);
			WritableByteChannel channel = Channels.newChannel(ios);

			ByteBuffer src2 = src.duplicate();
			src2.limit(src2.position() + comp_length);
			src.position(src.position() + comp_length);

			inflater.setDictionary(dict.data, dict.off, dict.len);
			channel.write(src2);
			ios.finish();
		}
	}

	public static byte[] toByteArray(ByteBuffer org) {
		if (org.hasArray()) return org.array();
		byte[] buf = new byte[org.remaining()];
		org.get(buf);
		return buf;
	}

	private static class Dictionary {
		final byte[] data;
		final int off, len;

		public Dictionary(byte[] data, int off, int len) {
			this.data = data;
			this.off = off;
			this.len = len;
		}
	}

	//======================================================================

	public interface Access {
		long getSize();
		ByteBuffer pread(long offset, long size);
	}

}
