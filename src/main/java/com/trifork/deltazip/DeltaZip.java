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

	private static final int VERSION_SIZE_BITS = 28;
	private static final int VERSION_SIZE_LIMIT = 1 << VERSION_SIZE_BITS;

	private static final int METHOD_UNCOMPRESSED = 0;
	private static final int METHOD_CHUNKED_DEFLATE = 2;

	protected static final CompressionMethod[] COMPRESSION_METHODS;
	protected static final CompressionMethod UNCOMPRESSED_INSTANCE = new UncompressedMethod();
	protected static final CompressionMethod CHUNKED_DEFLATE_INSTANCE = new ChunkedDeflateMethod();
	static {
		COMPRESSION_METHODS = new CompressionMethod[16];
		insertCM(COMPRESSION_METHODS, UNCOMPRESSED_INSTANCE);
		insertCM(COMPRESSION_METHODS, CHUNKED_DEFLATE_INSTANCE);
	}
	private static void insertCM(CompressionMethod[] table, CompressionMethod cm) {
		table[cm.methodNumber()] = cm;
	}
	

	//==================== Fields ==========================================

	private final Inflater inflater = new Inflater(true);
	private final Access access;

	private long       current_pos, current_size;
	private int        current_method;
	private byte[]     current_version;
	private ByteBuffer exposed_current_version;

	//==================== API ==========================================
	
	public DeltaZip(Access access) throws IOException {
		this.access = access;
		set_cursor_at_end();
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

	/** Computes an AppendSpecification for adding a version.
	 *  Has the side effect of placing the cursor at the end.
	 */
	public AppendSpecification add(ByteBuffer new_version) throws IOException {
		set_cursor_at_end();
		ExtByteArrayOutputStream baos = new ExtByteArrayOutputStream();
		ByteBuffer last_version = get();
		if (last_version != null) {
			pack_compressed(last_version, toByteArray(new_version), baos);
		}
		pack_uncompressed(new_version, baos);
		return new AppendSpecification(current_pos, baos.toByteArray());
	}

	//==================== Internals =======================================

	protected void set_cursor_at_end() throws IOException {
		set_initial_position();
		if (hasPrevious()) previous();
	}

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

	protected void pack_uncompressed(ByteBuffer version, ExtByteArrayOutputStream dst) {
		pack_entry(version, null, UNCOMPRESSED_INSTANCE, dst);
	}
	protected void pack_compressed(ByteBuffer version, byte[] ref_version, ExtByteArrayOutputStream dst) {
		pack_entry(version, ref_version, CHUNKED_DEFLATE_INSTANCE, dst);
	}

	protected void pack_entry(ByteBuffer version, byte[] ref_version, CompressionMethod cm, ExtByteArrayOutputStream dst) {
		int tag_blank = dst.insertBlank(2);
		int size_before = dst.size();
		cm.compress(version, ref_version, dst);
		int size_after = dst.size();
		int length = size_after - size_before;
		if (length >= VERSION_SIZE_LIMIT) throw new IllegalArgumentException("Version is too big");
		int tag = (cm.methodNumber() << VERSION_SIZE_BITS) | length;
		dst.fillBlankWithBigEndianInteger(tag_blank, tag, 2);
	}

	//==================== Compression methods =============================
	protected static abstract class CompressionMethod {
		public abstract int methodNumber();
		public abstract void compress(ByteBuffer org, byte[] ref_data, OutputStream dst);
		public abstract byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) throws IOException;
	}

	protected static class UncompressedMethod extends CompressionMethod {
		public int methodNumber() {return METHOD_UNCOMPRESSED;}

		public void compress(ByteBuffer org, byte[] ref_data, OutputStream dst) {
			try {
				WritableByteChannel channel = Channels.newChannel(dst);
				channel.write(org);
			} catch (IOException ioe) {throw new RuntimeException(ioe);}
		}
		public byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) {return toByteArray(org);}
	}

	protected static class ChunkedDeflateMethod extends CompressionMethod {
		public int methodNumber() {return METHOD_CHUNKED_DEFLATE;}

		private static final int CHUNK_METHOD_DEFLATE = 0;
		private static final int CHUNK_METHOD_PREFIX_COPY = 1;
		private static final int CHUNK_METHOD_OFFSET_COPY = 2;

		public void compress(ByteBuffer org, byte[] ref_data, OutputStream dst) {
			//TODO
		}

		public byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) throws IOException {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();

			int ref_data_offset = 0;
			while (org.hasRemaining()) {
				System.err.println("DB| uncompress: remaining="+org.remaining());
				// Parse chunk header:
				int meth = org.get();
				int meth_major = meth >> 3;
				switch (meth_major) {
				case CHUNK_METHOD_DEFLATE: {
					int rskip_spec = meth & 7;
					int comp_data_size = org.getChar(); // unsigned short

					// Determine and set dictionary:
					int rskip = spec_to_rskip(rskip_spec);
					ref_data_offset += rskip;
					int dict_size = Math.min(WINDOW_SIZE, ref_data.length-ref_data_offset);
					Dictionary dict = new Dictionary(ref_data, ref_data_offset, dict_size);

					// Inflate:
					int before = baos.size();
					inflate(inflater, org, comp_data_size, baos, dict);
					int after = baos.size();
					System.err.println("DB| inflated "+comp_data_size+" to "+(after-before));
				} break;
				case CHUNK_METHOD_PREFIX_COPY: {
					if ((meth & 7) != 0) throw new IOException("Invalid chunk encoding: "+meth);
					int comp_data_size = org.getChar(); // unsigned short
					if (comp_data_size != 2) throw new IOException("Invalid chunk length: "+comp_data_size);

					int copy_length = 1 + org.getChar(); // unsigned short
					baos.write(ref_data, ref_data_offset, copy_length);
					ref_data_offset += copy_length;
				} break;
				case CHUNK_METHOD_OFFSET_COPY: {
					if ((meth & 7) != 0) throw new IOException("Invalid chunk encoding: "+meth);
					int comp_data_size = org.getChar(); // unsigned short
					if (comp_data_size != 4) throw new IOException("Invalid chunk length: "+comp_data_size);

					int offset      = 1 + org.getChar(); // unsigned short
					int copy_length = 1 + org.getChar(); // unsigned short
					ref_data_offset += offset;
					baos.write(ref_data, ref_data_offset, copy_length);
					ref_data_offset += copy_length;
				} break;
				default:
					throw new IOException("Invalid chunk encoding: "+meth);
				}//switch
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

	protected static class ExtByteArrayOutputStream extends ByteArrayOutputStream {
		public int insertBlank(int len) {
			int pos = count;
			for (int i=0; i<len; i++) write(0);
			return pos;
		}

		public void fillBlank(int pos, byte[] data) {
			for (int i=0; i<data.length; i++) {
				buf[pos+i] = data[i];
			}
		}

		public void fillBlankWithBigEndianInteger(int pos, int value, int len) {
			if (len>4) throw new IllegalArgumentException("length is > 4");
			for (int i=len-1; i>=0; i--) {
				buf[pos++] = (byte) (value >> (8*i));
			}
		}
	}

	//==================== Interface types ==============================

	public interface Access {
		long getSize();
		ByteBuffer pread(long offset, long size);
	}

	public final class AppendSpecification {
		final long prefix_size;
		final ByteBuffer new_tail;

		public AppendSpecification(long prefix_size, ByteBuffer new_tail) {
			this.prefix_size = prefix_size;
			this.new_tail = new_tail.asReadOnlyBuffer();
		}

		public AppendSpecification(long prefix_size, byte[] new_tail) {
			this(prefix_size, ByteBuffer.wrap(new_tail));
		}
	}

}
