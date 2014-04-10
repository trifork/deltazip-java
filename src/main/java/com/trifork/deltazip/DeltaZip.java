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

    public static final boolean USE_JZLIB_ALWAYS = System.getProperty("deltazip-use-jzlib-always") != null;

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

	private final Access access;
    private FormatVersion format_version;
    private final long archive_size;

	//==================== API ==========================================
	
	public DeltaZip(Access access) throws IOException {
		this.access = access;
		this.format_version = check_magic_header();
        this.archive_size = access.getSize();
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
    public AppendSpecification add(ByteBuffer new_version, List<Metadata.Item> metadata) throws IOException {
        return add(new Version(new_version, metadata));
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
    public AppendSpecification add(Iterable<Version> versions_to_add) throws IOException {
        return add(versions_to_add.iterator());
    }

    /** Computes an AppendSpecification for adding a version.
     *  Has the side effect of placing the cursor at the end.
     */
	public AppendSpecification add(Iterator<Version> versions_to_add) throws IOException {
		ExtByteArrayOutputStream baos = new ExtByteArrayOutputStream();

        VersionIterator iter = backwardIterator();
        Version prev_version = iter.hasNext() ? iter.next() : null;
        long current_pos = iter.getCurrentPosition();

        // If the file is empty, add a header:
        if (current_pos ==0) baos.writeBigEndianInteger(DELTAZIP_MAGIC_HEADER | VERSION_11, 4);

        if (!versions_to_add.hasNext()) { // Handle degenerate case.
            return new AppendSpecification(archive_size, baos.toByteArray());
        }

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

    /** Return the most recent version, or null if the archive is empty. */
    public Version latestVersion() {
        VersionIterator iter = backwardIterator();
        return iter.hasNext() ? iter.next() : null;
    }

    //========== Iteration: ==========

    public Iterable<Version> backwardIterable() {
        return new Iterable() {
            @Override
            public Iterator iterator() {
                return backwardIterator();
            }
        };
    }

    public VersionIterator backwardIterator() {
        return new BackwardIterator();
    }

    public Iterable<List<Metadata.Item>> backwardMetadataIterable() {
        return new Iterable() {
            @Override
            public MetadataIterator iterator() {
                return backwardMetadataIterator();
            }
        };
    }

    public MetadataIterator backwardMetadataIterator() {
        return new BackwardMetadataIterator();
    }

    public Iterable<List<Metadata.Item>> forwardMetadataIterable() {
        return new Iterable() {
            @Override
            public MetadataIterator iterator() {
                return forwardMetadataIterator();
            }
        };
    }

    public MetadataIterator forwardMetadataIterator() {
        return new ForwardMetadataIterator();
    }

    //==================== Internals =======================================

    /** @returns the archive format version number. */
	protected FormatVersion check_magic_header() throws IOException {
		if (archive_size == 0) return FormatVersion.VERSION_11; // OK (empty)
        int magic_header = read_magic_header();
        if (archive_size < FILE_HEADER_LENGTH ||
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

	private static void dump(String s, byte[] buf) {
		System.err.print(s);
		System.err.print("<<");
		for (int i=0; i<buf.length; i++) {
			if (i>0) System.err.print(",");
			System.err.print(buf[i] & 0xff);
		}
		System.err.println(">>");
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

		try { // Because of the (technical...) possibility of IOExceptions from ByteArrayOutputStream...
            // Write metadata:
            List<Metadata.Item> metadata = version.getMetadata();
            final boolean has_metadata = !metadata.isEmpty();
            if (has_metadata) {
                if (!format_version.supportsMetadata()) throw new IllegalArgumentException("Archive format version does not support metadata.");
                Metadata.pack(metadata, dst);
            }

            CompressionMethod selected_method = null;
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

            // Compute length of envelope contents:
            int size_after = dst.size();
            int length = size_after - size_before;
            if (length >= format_version.versionSizeLimit()) throw new IllegalArgumentException("Version is too big to store");

            // Write tag at both ends of the envelope:
            int tag = (selected_method.methodNumber() << METHOD_BIT_POSITION) | length;
            if (has_metadata) tag |= (1 << METADATA_FLAG_BIT_POSITION);
            tag_gap.fillWithBigEndianInteger(tag, 4);
            dst.writeBigEndianInteger(tag, 4);

        } catch (IOException ioe) {
			// Shouldn't happen; it's a ByteArrayOutputStream.
			throw new RuntimeException(ioe);
		}

	}

    //==================== Iteration implementation ==============================

    public abstract class IteratorBase {
        protected static final int ENVELOPE_HEADER  = 4 + 4; // Start-tag + checksum
        protected static final int ENVELOPE_TRAILER = 4; // End.tag
        protected static final int ENVELOPE_OVERHEAD = ENVELOPE_HEADER + ENVELOPE_TRAILER;
        protected long       current_pos;
        protected int        current_size;
        protected int        current_method;
        protected int        current_checksum;
        protected boolean current_has_metadata;

        public long getCurrentPosition() {return current_pos;}
        public int getCurrentChecksum() {return current_checksum;}

        protected boolean thereIsAPreviousVersion() {
            return current_pos > FILE_HEADER_LENGTH;
        }
        protected boolean thereIsANextVersion() {
            return current_pos + current_size + ENVELOPE_OVERHEAD < archive_size;
        }

        //========== Cursor manipulation:

        protected void goto_previous_position_and_read_header() throws IOException {
            // Read envelope trailer:
            ByteBuffer trailer_buf = access.pread(current_pos-ENVELOPE_TRAILER, ENVELOPE_TRAILER);
            int tag = trailer_buf.getInt(0);
            int size = extractSizeFromTag(tag);
            long start_pos = current_pos - size - ENVELOPE_OVERHEAD;

            // Read envelope header:
            ByteBuffer header_buf = access.pread(start_pos, ENVELOPE_HEADER);
            header_buf.rewind();
            int tag2 = header_buf.getInt();
            int adler32 = header_buf.getInt();

            set_cursor_common(tag, tag2, start_pos, size, adler32);
        }

        protected void goto_next_position_and_read_header() throws IOException {
            long start_pos = (current_pos==0) ? FILE_HEADER_LENGTH : current_pos + current_size + ENVELOPE_OVERHEAD;

            // Read envelope header:
            ByteBuffer header_buf = access.pread(start_pos, ENVELOPE_HEADER);
            header_buf.rewind();
            int tag = header_buf.getInt();
            int size = extractSizeFromTag(tag);
            int adler32 = header_buf.getInt();

            // Read envelope trailer:
            ByteBuffer trailer_buf = access.pread(start_pos + ENVELOPE_HEADER + size, ENVELOPE_TRAILER);
            int tag2 = trailer_buf.getInt(0);

            set_cursor_common(tag, tag2, start_pos, size, adler32);
        }

        private void set_cursor_common(int tag, int tag2, long start_pos, int size, int adler32) throws IOException {
            if (tag2 != tag) throw new IOException("Data error - tag mismatch @ "+start_pos+";"+current_pos);

            int method = (tag >> METHOD_BIT_POSITION) & 15;
            boolean has_metadata = format_version.supportsMetadata() &&
                    (tag & (1 << METADATA_FLAG_BIT_POSITION)) != 0;

            this.current_pos     = start_pos;
            this.current_method  = method;
            this.current_size    = size;
            this.current_checksum = adler32;
            this.current_has_metadata = has_metadata;
        }

        private int extractSizeFromTag(int tag) {
            return tag &~ (-1 << format_version.versionSizeBits());
        }

        protected List<Metadata.Item> extract_just_metadata() throws IOException {
            ByteBuffer data_buf = access.pread(current_pos + ENVELOPE_HEADER, current_size); // Room for improvement here.
            return current_has_metadata ? Metadata.unpack(data_buf) : Collections.EMPTY_LIST;
        }
    }

    private class BackwardMetadataIterator extends IteratorBase implements MetadataIterator {
        private List<Metadata.Item> current_metadata;

        public BackwardMetadataIterator() {
            this.current_pos = archive_size;
        }

        @Override
        public long getCurrentPosition() {return this.current_pos;}

        @Override
        public void remove() { throw new UnsupportedOperationException(); }

        @Override
        /** Tells whether there are older revisions. */
        public boolean hasNext() {
            return thereIsAPreviousVersion();
        }

        @Override
        /** Retreat the cursor.
         *  @throws InvalidStateException if the cursor is pointing at the first revision.
         */
        public List<Metadata.Item> next() {
            if (!hasNext()) throw new IllegalStateException();
            try {
                goto_previous_position_and_read_header();
                this.current_metadata = extract_just_metadata();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }

            return current_metadata==null ? null : Collections.unmodifiableList(current_metadata);
        }
    }

    private class ForwardMetadataIterator extends IteratorBase implements MetadataIterator {
        private List<Metadata.Item> current_metadata;

        public ForwardMetadataIterator() {
            this.current_pos = 0;
            this.current_size = 4;
        }

        @Override
        public long getCurrentPosition() {return this.current_pos;}

        @Override
        public void remove() { throw new UnsupportedOperationException(); }

        @Override
        /** Tells whether there are older revisions. */
        public boolean hasNext() {
            return thereIsANextVersion();
        }

        @Override
        /** Retreat the cursor.
         *  @throws InvalidStateException if the cursor is pointing at the first revision.
         */
        public List<Metadata.Item> next() {
            if (!hasNext()) throw new IllegalStateException();
            try {
                goto_next_position_and_read_header();
                this.current_metadata = extract_just_metadata();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }

            return current_metadata==null ? null : Collections.unmodifiableList(current_metadata);
        }
    }

    private class BackwardIterator extends IteratorBase implements VersionIterator {
        private final Inflater inflater = new Inflater(true);
        private byte[]     current_version;
        private ByteBuffer exposed_current_version;
        private List<Metadata.Item> current_metadata;

        public BackwardIterator() {
            this.current_pos = archive_size;
        }

        @Override
        public void remove() { throw new UnsupportedOperationException(); }

        @Override
        /** Tells whether there are older revisions. */
        public boolean hasNext() {
            return current_pos > FILE_HEADER_LENGTH;
        }

        public int getCurrentMethod()   {return current_method;}
        public int getCurrentCompSize() {return current_size;}
        public int getCurrentRawSize()  {return current_version==null? -1 : current_version.length;}

        @Override
        /** Retreat the cursor.
         *  @throws InvalidStateException if the cursor is pointing at the first revision.
         */
        public Version next() {
            if (!hasNext()) throw new IllegalStateException();
            try {
                goto_previous_position_and_compute_current_version();
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
            return currentVersion();
        }

        /** Get the revision pointed to by the cursor. */
        private Version currentVersion() {
            return new Version(getData(), getMetadata());
        }

        /** Get the revision pointed to by the cursor. */
        private ByteBuffer getData() {
            return (exposed_current_version==null) ? null
                    : exposed_current_version.duplicate();
        }

        /** Get the metadata associated with revision pointed to by the cursor. */
        private List<Metadata.Item> getMetadata() {
            return current_metadata==null ? null : Collections.unmodifiableList(current_metadata);
        }

        private void goto_previous_position_and_compute_current_version() throws ArchiveIntegrityException, IOException {
            goto_previous_position_and_read_header();

            ByteBuffer data_buf = access.pread(current_pos + ENVELOPE_HEADER, current_size);
            List<Metadata.Item> metadata =
                    current_has_metadata ? Metadata.unpack(data_buf) : Collections.EMPTY_LIST;

            // Unpack:
            byte[] version = compute_current_version(current_method, data_buf, current_pos);

            // Verify checksum:
            int actual_adler32 = DZUtil.computeAdler32(version);
            if (actual_adler32 != current_checksum) {
                dump("checksumming failed: "+actual_adler32+" rather than "+current_checksum, version);
                throw new IOException("Data error - checksum mismatch @ "+current_pos+": stored is "+current_checksum+" but computed is "+actual_adler32);
            }

            // Commit, part 2:
            this.current_version = version;
            this.exposed_current_version = ByteBuffer.wrap(current_version).asReadOnlyBuffer();
            this.current_metadata = metadata;
        }

        protected byte[] compute_current_version(int method, ByteBuffer data_buf, long pos) throws IOException {
            CompressionMethod cm = COMPRESSION_METHODS[method];
            if (cm==null) throw new IOException("Invalid compression method: "+method+" @ "+pos);

            return cm.uncompress(data_buf, current_version, inflater);
        }

    }

    //==================== Compression methods =============================
	protected static abstract class CompressionMethod {
		public abstract int methodNumber();
		public abstract void compress(ByteBuffer org, byte[] ref_data, OutputStream dst) throws IOException;
		public abstract byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) throws ArchiveIntegrityException;
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

    public interface VersionIterator extends Iterator<Version> {
        public long getCurrentPosition();
        public int getCurrentChecksum();
        public int getCurrentMethod();
        public int getCurrentCompSize();
        public int getCurrentRawSize();
    }

    public interface MetadataIterator extends Iterator<List<Metadata.Item>> {
        public long getCurrentPosition();
    }
}