package com.trifork.deltazip;

import java.util.ArrayList;

import java.util.zip.Deflater;
import java.util.zip.Inflater;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.WritableByteChannel;
import java.nio.channels.Channels;

import com.trifork.deltazip.DZUtil.Dictionary;

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

	private static final int CHUNK_METHOD_DEFLATE = 0;
	private static final int CHUNK_METHOD_PREFIX_COPY = 1;
	private static final int CHUNK_METHOD_OFFSET_COPY = 2;

	//==================== API fulfillment ==============================
	public int methodNumber() {return DeltaZip.METHOD_CHUNKED;}

	//==================== Uncompression: ========================================
	public byte[] uncompress(ByteBuffer org, byte[] ref_data, Inflater inflater) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		int ref_data_offset = 0;
		while (org.hasRemaining()) {
// 			System.err.println("DB| uncompress: remaining="+org.remaining());
			// Parse chunk header:
			int meth = org.get();
			int meth_major = meth >> 3;
// 			System.err.println("DB| uncompress: method="+meth);
			switch (meth_major) {
			case CHUNK_METHOD_DEFLATE: {
				int rskip_spec = meth & 7;
				int comp_data_size = org.getChar(); // unsigned short

				// Determine dictionary:
				int rskip = spec_to_rskip(rskip_spec);
				ref_data_offset += rskip;
				int dict_size = Math.min(WINDOW_SIZE, ref_data.length-ref_data_offset);
				Dictionary dict = new Dictionary(ref_data, ref_data_offset, dict_size);

				// Inflate:
				int before = baos.size();
				DZUtil.inflate(inflater, org, comp_data_size, baos, dict);
				int after = baos.size();
// 				System.err.println("DB| inflated "+comp_data_size+" to "+(after-before));
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
			Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION, true);

			int ref_data_offset = 0;
			while (org.hasRemaining()) {
				chunk_options.clear();

				// Generate chunk options:
				int save_pos = org.position();
				addIfApplicable(chunk_options, PrefixChunkOption.create(org, ref_data, ref_data_offset));
				org.position(save_pos);
				addIfApplicable(chunk_options, SuffixChunkOption.create(org, ref_data, ref_data_offset));
				org.position(save_pos);
				for (int dsize_spec=-1; dsize_spec<3; dsize_spec++)
					for (int rskip_spec=0; rskip_spec<4; rskip_spec++) {
						addIfApplicable(chunk_options,
										DeflateChunkOption.create(org, ref_data, ref_data_offset,
																  rskip_spec, dsize_spec, deflater));
						org.position(save_pos);
					}

				// Evaluate chunk options:
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
// 		System.err.println("DB| choosing chunk option "+best_candidate+" with ratio "+best_ratio);
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
	static class DeflateChunkOption extends ChunkOption {
		public static DeflateChunkOption create(ByteBuffer data, byte[] ref_data, int ref_data_offset,
												int rskip_spec, int dsize_spec, Deflater deflater)
		{
			int remaining_data = data.remaining();
			int remaining_ref = ref_data.length - ref_data_offset;
			boolean all_is_visible =
				remaining_data <= WINDOW_SIZE &&
				remaining_ref  <= WINDOW_SIZE;

			// If all is visible, try only total dsize:
			if (all_is_visible && dsize_spec != -1) return null;
			
			int uncomp_size = spec_to_dsize(dsize_spec, data.remaining());

			// Determine dictionary:
			int rskip = Math.min(spec_to_rskip(rskip_spec), remaining_ref);
			ref_data_offset += rskip;
			int dict_size = Math.min(WINDOW_SIZE, ref_data.length-ref_data_offset);
			Dictionary dict = new Dictionary(ref_data, ref_data_offset, dict_size);

			// Deflate:
			byte[] comp_data = DZUtil.deflate(deflater, data, uncomp_size, dict);

			return new DeflateChunkOption(rskip_spec, comp_data, uncomp_size);
		}

		//----------
		final int rskip_spec;
		byte[] comp_data;
		public DeflateChunkOption(int rskip_spec, byte[] comp_data, int uncomp_size) {
			super(comp_data.length, uncomp_size, spec_to_rskip(rskip_spec));
			this.rskip_spec = rskip_spec;
			this.comp_data = comp_data;
		}

		public int chunkMethod() {return (CHUNK_METHOD_DEFLATE << 3) | rskip_spec;}

		public void writeCompData(DataOutputStream dos) throws IOException {
			dos.write(comp_data);
		}

	}

	//==================== Common helpers =======================================

	public static int spec_to_rskip(int rskip_spec) {
		return rskip_spec * (CHUNK_SIZE / 2);
	}

	public static int spec_to_dsize(int dsize_spec, int total_dsize) {
		if (dsize_spec == -1) return total_dsize;
		else return (2+dsize_spec) * (CHUNK_SIZE / 2);
	}

}// class ChunkedMethod
