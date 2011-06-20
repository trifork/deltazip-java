package com.trifork.deltazip;

import java.util.ArrayList;

import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.WritableByteChannel;
import java.nio.channels.Channels;

class ChunkedMethod extends DeltaZip.CompressionMethod {
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

	//==================== API fulfillment ==============================
	public int methodNumber() {return DeltaZip.METHOD_CHUNKED;}

	private static final int CHUNK_METHOD_DEFLATE = 0;
	private static final int CHUNK_METHOD_PREFIX_COPY = 1;
	private static final int CHUNK_METHOD_OFFSET_COPY = 2;


	//==================== Uncompression: ========================================
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

	//==================== Compression: ========================================
	public void compress(ByteBuffer org, byte[] ref_data, OutputStream dst) {
		try {
			ArrayList<ChunkOption> chunk_options = new ArrayList<ChunkOption>();
			DataOutputStream dos = new DataOutputStream(dst);

			int ref_data_offset = 0;
			while (org.hasRemaining()) {
				System.err.println("DB| Chunking from ("+org.position()+","+ref_data_offset+")");
				System.err.println("DB| Remaining: ("+org.remaining()+","+(ref_data.length - ref_data_offset)+")");
				chunk_options.clear();

				int save_pos = org.position();
				addIfApplicable(chunk_options, PrefixChunkOption.create(org, ref_data, ref_data_offset));
				org.position(save_pos);
				addIfApplicable(chunk_options, SuffixChunkOption.create(org, ref_data, ref_data_offset));
				org.position(save_pos);

				ChunkOption chunk_option = findBestCandidate(chunk_options);
				chunk_option.write(dos);

				org.position(save_pos + chunk_option.uncomp_size);
				ref_data_offset += chunk_option.rskip;
			}
		} catch (IOException ioe) {throw new RuntimeException(ioe);}
	}

	protected static void addIfApplicable(ArrayList<ChunkOption> list, ChunkOption option) {
		if (option != null) list.add(option);
	}

	protected static ChunkOption findBestCandidate(Iterable<ChunkOption> chunk_options) {
		double best_ratio = Double.MAX_VALUE;
		ChunkOption best_candidate = null;
		for (ChunkOption co : chunk_options) {
			double candidate_ratio = co.ratio();
			if (candidate_ratio < best_ratio) {
				best_candidate = co;
				best_ratio = candidate_ratio;
			}
		}
		System.err.println("DB| choosing chunk option "+best_candidate+" with ratio "+best_ratio);
		return best_candidate;
	}

	static abstract class ChunkOption {
		public final int comp_size, uncomp_size;
		public final int rskip;

		public ChunkOption(int comp_size, int uncomp_size, int rskip) {
			this.comp_size = comp_size;
			this.uncomp_size = uncomp_size;
			this.rskip = rskip;
		}

		public double ratio() {
			final int OVERHEAD_PENALTY_BYTES = 30;
			return (comp_size + OVERHEAD_PENALTY_BYTES) / uncomp_size;
		}

		public final void write(DataOutputStream dos) throws IOException {
			dos.write(chunkMethod());
			dos.writeChar(comp_size);
			writeCompData(dos);
		}

		public abstract int chunkMethod();
		public abstract void writeCompData(DataOutputStream dos) throws IOException;
	}

	//==================== Concrete chunk options: ========================================

	//========== PrefixChunkOption ====================
	static class PrefixChunkOption extends ChunkOption {
		static final int SIZE_LIMIT = (1<<16);

		public static PrefixChunkOption create(ByteBuffer data, byte[] ref_data, int ref_data_offset) {
			int start_pos = data.position();
			int limit = Math.min(SIZE_LIMIT,
								 Math.min(data.remaining(), ref_data.length - ref_data_offset));
			int i = 0;
			if (limit>0) {
				System.err.println("PrefixChunkOption.create(): first data byte is "+data.get(start_pos));
				System.err.println("PrefixChunkOption.create(): first ref byte is "+ref_data[ref_data_offset]);
			}
			while (i < limit &&
				   data.get(start_pos + i) == ref_data[ref_data_offset + i]) {
				i++;
			}
			int prefix_length = i;

			return (prefix_length <= 0) ? null
				: new PrefixChunkOption(prefix_length);
		}

		public PrefixChunkOption(int prefix_length) {
			// Size of chunk contents is 2 bytes.
			super(2, prefix_length, prefix_length);
		}

		public int chunkMethod() {return CHUNK_METHOD_PREFIX_COPY << 3;}

		public void writeCompData(DataOutputStream dos) throws IOException {
			int prefix_length = this.uncomp_size;
			dos.writeShort((short)(prefix_length-1));
		}

		public String toString() {return "PrefixChunkOption("+uncomp_size+")";}
	}

	//========== SuffixChunkOption ====================
	static class SuffixChunkOption extends ChunkOption {
		static final int SIZE_LIMIT = (1<<16);

		public static SuffixChunkOption create(ByteBuffer data, byte[] ref_data, int ref_data_offset) {
			int end_pos = data.limit();
			int remaining_data = data.remaining();
			int remaining_ref = ref_data.length - ref_data_offset;
			int offset = remaining_ref - remaining_data;

			if (offset <= 0 || offset > SIZE_LIMIT) return null;

			int limit = Math.min(remaining_data, remaining_ref);
			int i = 0;
			while (i < limit &&
				   data.get(end_pos - 1 - i) == ref_data[ref_data.length - 1 - i]) {
				i++;
			}
			int suffix_length = i;
			if (suffix_length < remaining_data) return null; // First part of data is not covered.
			suffix_length = Math.min(suffix_length, SIZE_LIMIT);
				
			return (suffix_length <= 0) ? null
				: new SuffixChunkOption(offset, suffix_length);
		}

		public SuffixChunkOption(int offset, int suffix_length) {
			// Size of chunk contents is 4 bytes.
			super(4, suffix_length, offset+suffix_length);
		}

		public int chunkMethod() {return CHUNK_METHOD_OFFSET_COPY << 3;}

		public void writeCompData(DataOutputStream dos) throws IOException {
			int suffix_length = this.uncomp_size;
			int offset = this.rskip - suffix_length;
			dos.writeShort((short)(offset-1));
			dos.writeShort((short)(suffix_length-1));
		}

		public String toString() {
			int suffix_length = this.uncomp_size;
			int offset = this.rskip - suffix_length;
			return "SuffixChunkOption("+offset+","+suffix_length+")";
		}
	} // Class SuffixChunkOption

	//========== DeflateChunkOption ====================
	//TODO

	//==================== Common helpers =======================================

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

	private static class Dictionary {
		final byte[] data;
		final int off, len;
		
		public Dictionary(byte[] data, int off, int len) {
			this.data = data;
			this.off = off;
			this.len = len;
		}
	}

}// class ChunkedMethod
