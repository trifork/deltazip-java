package com.trifork.deltazip;

import java.util.Iterator;
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

	public static final int DELTAZIP_MAGIC_HEADER = 0xCEB47A10;
	public static final int FILE_HEADER_LENGTH = 4;

	// Snapshot methods (0-3):
	public static final int METHOD_UNCOMPRESSED   = 0;
	public static final int METHOD_DEFLATED       = 1;
	// Delta methods (4-15):
	public static final int METHOD_CHUNKED        = 4;
	public static final int METHOD_CHUNKED_MIDDLE = 5;


	private static final int VERSION_SIZE_BITS = 28;
	private static final int VERSION_SIZE_LIMIT = 1 << VERSION_SIZE_BITS;

	protected static final CompressionMethod[] COMPRESSION_METHODS;
	protected static final CompressionMethod UNCOMPRESSED_INSTANCE = new UncompressedMethod();
	protected static final CompressionMethod DEFLATED_INSTANCE = new DeflatedMethod();
	protected static final CompressionMethod CHUNKED_INSTANCE = new ChunkedMethod();
	protected static final CompressionMethod CHUNKED_MIDDLE_INSTANCE = new ChunkedMiddleMethod();
	static {
		COMPRESSION_METHODS = new CompressionMethod[16];
		insertCM(COMPRESSION_METHODS, UNCOMPRESSED_INSTANCE);
		insertCM(COMPRESSION_METHODS, DEFLATED_INSTANCE);
		insertCM(COMPRESSION_METHODS, CHUNKED_INSTANCE);
		insertCM(COMPRESSION_METHODS, CHUNKED_MIDDLE_INSTANCE);
	}
	private static void insertCM(CompressionMethod[] table, CompressionMethod cm) {
		table[cm.methodNumber()] = cm;
	}
	

	//==================== Fields ==========================================

	private final Inflater inflater = new Inflater(true);
	private final Access access;

	private long       current_pos;
	private int        current_size;
	private int        current_method;
	private byte[]     current_version;
	private int        current_checksum;
	private ByteBuffer exposed_current_version;

	//==================== API ==========================================
	
	public DeltaZip(Access access) throws IOException {
		this.access = access;
		check_magic_header();
		set_cursor_at_end();
	}

	/** For cloning. */
	private DeltaZip(DeltaZip org) {
		this.access = org.access;

		this.current_pos = org.current_pos;
		this.current_size = org.current_size;
		this.current_method = org.current_method;
		this.current_version = org.current_version;
		this.current_checksum = org.current_checksum;
		this.exposed_current_version = org.exposed_current_version;
	}

	/** Get the revision pointed to by the cursor. */
	public ByteBuffer get() {
		return (exposed_current_version==null) ? null
			: exposed_current_version.duplicate();
	}

	/** Tells whether there are older revisions. */
	public boolean hasPrevious() {
		return current_pos > FILE_HEADER_LENGTH;
	}

	/** Retreat the cursor.
	 *  @throws InvalidStateException if the cursor is pointing at the first revision.
	 */
	public void previous() throws IOException {
		if (!hasPrevious()) throw new IllegalStateException();
		goto_previous_position_and_compute_current_version();
	}

	/** Set the cursor to point at the end of the archive.
	 */
	public void resetCursor() throws IOException {
		if (!at_initial_position()) set_cursor_at_end();
	}

	public DeltaZip clone() {
		return new DeltaZip(this);
	}

	/** Computes an AppendSpecification for adding a version.
	 *  Has the side effect of placing the cursor at the end.
	 */
	public AppendSpecification add(ByteBuffer new_version) throws IOException {
		set_cursor_at_end();
		ExtByteArrayOutputStream baos = new ExtByteArrayOutputStream();

		// If the file is empty, add a header:
		if (current_pos==0) baos.writeBigEndianInteger(DELTAZIP_MAGIC_HEADER, 4);

		ByteBuffer last_version = get();

		if (last_version != null) {
			pack_delta(last_version, DZUtil.allToByteArray(new_version), baos);
		}
		pack_snapshot(new_version, baos);

		return new AppendSpecification(current_pos, baos.toByteArray());
	}

	/** Computes an AppendSpecification for adding a version.
	 *  Has the side effect of placing the cursor at the end.
	 */
	public AppendSpecification add(Iterator<ByteBuffer> versions_to_add) throws IOException {
		set_cursor_at_end();
		ExtByteArrayOutputStream baos = new ExtByteArrayOutputStream();

		// If the file is empty, add a header:
		if (current_pos==0) baos.writeBigEndianInteger(DELTAZIP_MAGIC_HEADER, 4);

		ByteBuffer prev_version = get();

		while (versions_to_add.hasNext()) {
			ByteBuffer cur = versions_to_add.next();
			if (prev_version != null) {
				pack_delta(prev_version, DZUtil.allToByteArray(cur), baos);
			}
			prev_version = cur;
		}

		pack_snapshot(prev_version, baos);

		return new AppendSpecification(current_pos, baos.toByteArray());
	}

	//==================== Stats API =======================================
	public int getCurrentChecksum() {return current_checksum;}
	public int getCurrentMethod()   {return current_method;}
	public int getCurrentCompSize() {return current_size;}
	public int getCurrentRawSize()  {return current_version==null? -1 : current_version.length;}

	//==================== Internals =======================================

	protected void check_magic_header() throws IOException {
		long size = access.getSize();
		if (size == 0) return; // OK (empty)
		if (size < FILE_HEADER_LENGTH ||
			read_magic_header() != DELTAZIP_MAGIC_HEADER)
			throw new IOException("Not a deltazip file (invalid header)");
	}

	protected int read_magic_header() throws IOException {
		ByteBuffer header = access.pread(0,4);
		int magic = header.getInt(0);
		return magic;
	}

	protected void set_cursor_at_end() throws IOException {
		set_initial_position();
		if (hasPrevious()) previous();
	}

	protected void set_initial_position() throws IOException {
		current_pos = access.getSize();
	}

	protected boolean at_initial_position() throws IOException {
		return current_pos == access.getSize();
	}

	private static final int ENVELOPE_HEADER  = 4 + 4; // Start-tag + checksum
	private static final int ENVELOPE_TRAILER = 4; // End.tag
	private static final int ENVELOPE_OVERHEAD = ENVELOPE_HEADER + ENVELOPE_TRAILER;
	protected void goto_previous_position_and_compute_current_version() throws IOException {
		ByteBuffer tag_buf = access.pread(current_pos-ENVELOPE_TRAILER, ENVELOPE_TRAILER);
		int tag = tag_buf.getInt(0);
		int size = tag &~ (-1 << 28);
		int method = (tag >> 28) & 15;
// 		System.err.println("DB| tag="+tag+" -> "+method+":"+size);

		// Read envelope header:
		long start_pos = current_pos - size - ENVELOPE_OVERHEAD;
		ByteBuffer data_buf = access.pread(start_pos, size + ENVELOPE_HEADER);
		data_buf.rewind();
		int start_tag = data_buf.getInt();
		if (start_tag != tag) throw new IOException("Data error - tag mismatch @ "+start_pos+";"+current_pos);
		int adler32 = data_buf.getInt();

		// Unpack:
		byte[] version = compute_current_version(method, data_buf, start_pos);

		// Verify checksum:
		int actual_adler32 = DZUtil.computeAdler32(version);
		if (actual_adler32 != adler32) {
			dump("checksumming failed: "+actual_adler32+" rather than "+adler32, version);
			throw new IOException("Data error - checksum mismatch @ "+start_pos+": stored is "+adler32+" but computed is "+actual_adler32);
		}

		// Commit:
		this.current_pos     = start_pos;
		this.current_method  = method;
		this.current_size    = size;
		this.current_version = version;
		this.exposed_current_version = ByteBuffer.wrap(current_version).asReadOnlyBuffer();
		this.current_checksum = actual_adler32;
	}
	public static void dump(String s, byte[] buf) {
		System.err.print(s);
		System.err.print("<<");
		for (int i=0; i<buf.length; i++) {
			if (i>0) System.err.print(",");
			System.err.print(buf[i] & 0xff);
		}
		System.err.println(">>");
	}

	protected byte[] compute_current_version(int method, ByteBuffer data_buf, long pos) throws IOException {
		CompressionMethod cm = COMPRESSION_METHODS[method];
		if (cm==null) throw new IOException("Invalid compression method: "+method+" @ "+pos);

		return cm.uncompress(data_buf, current_version, inflater);
	}

	//====================

	protected void pack_snapshot(ByteBuffer version, ExtByteArrayOutputStream dst) {
		pack_entry(version, null, DEFLATED_INSTANCE, dst);
	}

	protected void pack_delta(ByteBuffer version, byte[] ref_version, ExtByteArrayOutputStream dst) {
		pack_entry(version, ref_version, CHUNKED_MIDDLE_INSTANCE, dst);
	}

	//====================

	protected void pack_entry(ByteBuffer version, byte[] ref_version, CompressionMethod cm, ExtByteArrayOutputStream dst) {
		int adler32 = DZUtil.computeAdler32(version);

		int tag_blank = dst.insertBlank(4);
		dst.writeBigEndianInteger(adler32, 4);

		int size_before = dst.size();
		try {
			cm.compress(version.duplicate(), ref_version, dst);
		} catch (IOException ioe) {
			// Shouldn't happen; it's a ByteArrayOutputStream.
			throw new RuntimeException(ioe);
		}
		int size_after = dst.size();
		int length = size_after - size_before;

		if (length >= VERSION_SIZE_LIMIT) throw new IllegalArgumentException("Version is too big to store");
		int tag = (cm.methodNumber() << VERSION_SIZE_BITS) | length;
		dst.fillBlankWithBigEndianInteger(tag_blank, tag, 4);
		dst.writeBigEndianInteger(tag, 4);
	}


	//========== ByteArrayOutputStream with 'blanks' support ==========

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

	//==================== Compression methods =============================
	protected static abstract class CompressionMethod {
		public abstract int methodNumber();
		public abstract void compress(ByteBuffer org, byte[] ref_data, OutputStream dst) throws IOException;
		public abstract byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) throws IOException;
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
			this(prefix_size, ByteBuffer.wrap(new_tail).asReadOnlyBuffer());
		}
	}

}
