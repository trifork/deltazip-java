package com.trifork.deltazip;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.zip.Inflater;

import java.nio.ByteBuffer;
import java.io.OutputStream;
import java.io.IOException;

import static com.trifork.deltazip.ExtByteArrayOutputStream.Gap;

public class DeltaZip {

	//==================== Constants =======================================

	public static final int DELTAZIP_MAGIC_HEADER = 0xCEB47A00;
	public static final int MACIC_MASK = 0xFFFFFF00;
	public static final int VERSION_MASK = 0xFF;
	public static final int VERSION_10 = 0x10;
	public static final int VERSION_11 = 0x11;
	public static final int FILE_HEADER_LENGTH = 4;

	// Snapshot methods (0-3):
	public static final int METHOD_UNCOMPRESSED   = 0;
	public static final int METHOD_DEFLATED       = 1;
	// Delta methods (4-15):
	public static final int METHOD_CHUNKED        = 4;
    public static final int METHOD_CHUNKED_MIDDLE = 5;
    public static final int METHOD_CHUNKED_MIDDLE2= 7;


    private static int METHOD_BIT_POSITION = 28;
    private static int METADATA_FLAG_BIT_POSITION = 27;
	protected static final CompressionMethod[] COMPRESSION_METHODS;
	protected static final CompressionMethod UNCOMPRESSED_INSTANCE = new UncompressedMethod();
	protected static final CompressionMethod DEFLATED_INSTANCE = new DeflatedMethod();
	protected static final CompressionMethod CHUNKED_INSTANCE = new ChunkedMethod();
	protected static final CompressionMethod CHUNKED_MIDDLE_INSTANCE = new ChunkedMiddleMethod();
	protected static final CompressionMethod CHUNKED_MIDDLE2_INSTANCE = new ChunkedMiddle2Method();
	static {
		COMPRESSION_METHODS = new CompressionMethod[16];
		insertCM(COMPRESSION_METHODS, UNCOMPRESSED_INSTANCE);
		insertCM(COMPRESSION_METHODS, DEFLATED_INSTANCE);
		insertCM(COMPRESSION_METHODS, CHUNKED_INSTANCE);
        insertCM(COMPRESSION_METHODS, CHUNKED_MIDDLE_INSTANCE);
        insertCM(COMPRESSION_METHODS, CHUNKED_MIDDLE2_INSTANCE);
    }
	private static void insertCM(CompressionMethod[] table, CompressionMethod cm) {
		table[cm.methodNumber()] = cm;
	}
	

	//==================== Fields ==========================================

	private final Inflater inflater = new Inflater(true);
	private final Access access;

    private FormatVersion format_version;
	private long       current_pos;
	private int        current_size;
	private int        current_method;
	private byte[]     current_version;
	private int        current_checksum;
	private ByteBuffer exposed_current_version;
    private List<Metadata.Item> current_metadata;

	//==================== API ==========================================
	
	public DeltaZip(Access access) throws IOException {
		this.access = access;
		this.format_version = check_magic_header();
		set_cursor_at_end();
	}

	/** For cloning. */
	private DeltaZip(DeltaZip org) {
        // Archive-global fields:
		this.access = org.access;
        this.format_version = org.format_version;

        // Version-local fields:
		this.current_pos = org.current_pos;
		this.current_size = org.current_size;
		this.current_method = org.current_method;
		this.current_version = org.current_version;
		this.current_checksum = org.current_checksum;
		this.exposed_current_version = org.exposed_current_version;
        this.current_metadata = org.current_metadata;
	}

    /** Get the revision pointed to by the cursor. */
    public Version getVersion() {
        ByteBuffer contents = get();
        return contents==null ? null : new Version(contents, getMetadata());
    }

    /** Get the revision pointed to by the cursor. */
	public ByteBuffer get() {
		return (exposed_current_version==null) ? null
			: exposed_current_version.duplicate();
	}

	/** Get the metadata associated with revision pointed to by the cursor. */
	public List<Metadata.Item> getMetadata() {
		return current_metadata==null ? null : Collections.unmodifiableList(current_metadata);
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
        return add(new Version(new_version));
    }

	/** Computes an AppendSpecification for adding a version.
	 *  Has the side effect of placing the cursor at the end.
	 */
	public AppendSpecification add(Version new_version) throws IOException {
        return add(Collections.singletonList(new_version).iterator());
	}

	/** Computes an AppendSpecification for adding a version.
	 *  Has the side effect of placing the cursor at the end.
	 */
	public AppendSpecification add(Iterator<Version> versions_to_add) throws IOException {
		set_cursor_at_end();
		ExtByteArrayOutputStream baos = new ExtByteArrayOutputStream();

		// If the file is empty, add a header:
		if (current_pos==0) baos.writeBigEndianInteger(DELTAZIP_MAGIC_HEADER | VERSION_11, 4);

        Version prev_version = getVersion();

		while (versions_to_add.hasNext()) {
			Version cur = versions_to_add.next();
			if (prev_version != null) {
				pack_delta(prev_version, DZUtil.allToByteArray(cur.getContents()), baos);
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

    /** @returns the archive format version number. */
	protected FormatVersion check_magic_header() throws IOException {
		long size = access.getSize();
		if (size == 0) return FormatVersion.VERSION_11; // OK (empty)
        int magic_header = read_magic_header();
        if (size < FILE_HEADER_LENGTH ||
			(magic_header & MACIC_MASK) != DELTAZIP_MAGIC_HEADER)
			throw new IOException("Not a deltazip file (invalid header)");
        int version = magic_header & VERSION_MASK;
        if (version == VERSION_10) return FormatVersion.VERSION_10;
        if (version == VERSION_11) return FormatVersion.VERSION_11;
        throw new IOException("Not a readable deltazip file (unrecognized format version number)");
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
		int size = tag &~ (-1 << format_version.versionSizeBits());
		int method = (tag >> METHOD_BIT_POSITION) & 15;
        boolean has_metadata = format_version.supportsMetadata() &&
                (tag & (1 << METADATA_FLAG_BIT_POSITION)) != 0;
// 		System.err.println("DB| tag="+tag+" -> "+method+":"+size);

		// Read envelope header:
		long start_pos = current_pos - size - ENVELOPE_OVERHEAD;
		ByteBuffer data_buf = access.pread(start_pos, size + ENVELOPE_HEADER);
		data_buf.rewind();
		int start_tag = data_buf.getInt();
		if (start_tag != tag) throw new IOException("Data error - tag mismatch @ "+start_pos+";"+current_pos);
		int adler32 = data_buf.getInt();
        List<Metadata.Item> metadata =
                has_metadata ? Metadata.unpack(data_buf) : Collections.EMPTY_LIST;

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
        this.current_metadata = metadata;
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

    private final CompressionMethod[] SNAPSHOT_METHODS = {DEFLATED_INSTANCE};
	protected void pack_snapshot(Version version, ExtByteArrayOutputStream dst) {
		pack_entry(version, null, SNAPSHOT_METHODS, dst);
	}

    private final CompressionMethod[] DELTA_METHODS = {CHUNKED_MIDDLE_INSTANCE, CHUNKED_MIDDLE2_INSTANCE};
    protected void pack_delta(Version version, byte[] ref_version, ExtByteArrayOutputStream dst) {
        pack_entry(version, ref_version, DELTA_METHODS, dst);
	}

	//====================

	protected void pack_entry(Version version, byte[] ref_version, CompressionMethod[] cms, ExtByteArrayOutputStream dst) {
        // Write start of envelope:
        ByteBuffer version_data = version.getContents();
        int adler32 = DZUtil.computeAdler32(version_data);
        Gap tag_gap = dst.insertGap(4);
		dst.writeBigEndianInteger(adler32, 4);

		int size_before = dst.size();
        CompressionMethod selected_method = null;
		try {
            if (cms.length==1) { // Optimization: write directly.
                selected_method = cms[0];
                selected_method.compress(version_data.duplicate(), ref_version, dst);
            } else { // Try each method and select the most compact result.
                ExtByteArrayOutputStream best_out = new ExtByteArrayOutputStream();
                ExtByteArrayOutputStream candidate_out = new ExtByteArrayOutputStream();
                int best_size = Integer.MAX_VALUE;
                for (CompressionMethod cm : cms) {
                    candidate_out.reset();
                    cm.compress(version_data.duplicate(), ref_version, candidate_out);
                    int cand_size = candidate_out.size();
                    if (cand_size < best_size) { // Candidate is hitherto best.
                        // Swap 'best_out' and 'candidate_out':
                        ExtByteArrayOutputStream tmp=best_out; best_out=candidate_out; candidate_out=tmp;

                        best_size = cand_size;
                        selected_method = cm;
                    }
                }

                // Write the most compact result out.
                best_out.writeTo(dst);
            }
		} catch (IOException ioe) {
			// Shouldn't happen; it's a ByteArrayOutputStream.
			throw new RuntimeException(ioe);
		}

        // Compute length of envelope contents:
		int size_after = dst.size();
		int length = size_after - size_before;
		if (length >= format_version.versionSizeLimit()) throw new IllegalArgumentException("Version is too big to store");

        // Write tag at both ends of the envelope:
		int tag = (selected_method.methodNumber() << METHOD_BIT_POSITION) | length;
		tag_gap.fillWithBigEndianInteger(tag, 4);
		dst.writeBigEndianInteger(tag, 4);
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
