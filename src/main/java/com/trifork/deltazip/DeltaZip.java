package com.trifork.deltazip;

import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.Channels;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.io.IOException;

public class DeltaZip {

	//==================== Constants =======================================

	private static final int VERSION_SIZE_BITS = 28;
	private static final int VERSION_SIZE_LIMIT = 1 << VERSION_SIZE_BITS;

	public static final int METHOD_UNCOMPRESSED = 0;
	public static final int METHOD_CHUNKED = 2;

	protected static final CompressionMethod[] COMPRESSION_METHODS;
	protected static final CompressionMethod UNCOMPRESSED_INSTANCE = new UncompressedMethod();
	protected static final CompressionMethod CHUNKED_INSTANCE = new ChunkedMethod();
	static {
		COMPRESSION_METHODS = new CompressionMethod[16];
		insertCM(COMPRESSION_METHODS, UNCOMPRESSED_INSTANCE);
		insertCM(COMPRESSION_METHODS, CHUNKED_INSTANCE);
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
		return (exposed_current_version==null) ? null
			: exposed_current_version.duplicate();
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
		int save_pos = new_version.position();

		set_cursor_at_end();
		ExtByteArrayOutputStream baos = new ExtByteArrayOutputStream();
		ByteBuffer last_version = get();
		if (last_version != null) {
			pack_compressed(last_version, allToByteArray(new_version), baos);
		}
		pack_uncompressed(new_version, baos);

		new_version.position(save_pos); // Restore as-was.
		return new AppendSpecification(current_pos, baos.toByteArray());
	}

	//==================== Internals =======================================

	protected void set_cursor_at_end() throws IOException {
		set_initial_position();
		if (hasPrevious()) previous();
	}

	protected void set_initial_position() throws IOException {
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
		pack_entry(version, ref_version, CHUNKED_INSTANCE, dst);
	}

	protected void pack_entry(ByteBuffer version, byte[] ref_version, CompressionMethod cm, ExtByteArrayOutputStream dst) {
		int tag_blank = dst.insertBlank(4);
		int size_before = dst.size();
		cm.compress(version, ref_version, dst);
		int size_after = dst.size();
		int length = size_after - size_before;

		if (length >= VERSION_SIZE_LIMIT) throw new IllegalArgumentException("Version is too big to store");
		int tag = (cm.methodNumber() << VERSION_SIZE_BITS) | length;
		dst.fillBlankWithBigEndianInteger(tag_blank, tag, 4);
		dst.writeBigEndianInteger(tag, 4);
	}

	//==================== Compression methods =============================
	protected static abstract class CompressionMethod {
		public abstract int methodNumber();
		public abstract void compress(ByteBuffer org, byte[] ref_data, OutputStream dst);
		public abstract byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) throws IOException;
	}

	public static byte[] allToByteArray(ByteBuffer org) {
		int save_pos = org.position();
		org.position(0);
		byte[] buf = remainingToByteArray(org);
		org.position(save_pos);

		return buf;
	}

	public static byte[] remainingToByteArray(ByteBuffer org) {
		byte[] buf = new byte[org.remaining()];
		org.get(buf);
		return buf;
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

		public void writeBigEndianInteger(int value, int len) {
			for (int i=len-1; i>=0; i--) {
				write(value >> (8*i));
			}
		}
	}

	//==================== Interface types ==============================

	public interface Access {
		long getSize() throws IOException;
		ByteBuffer pread(long offset, int size) throws IOException;
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
