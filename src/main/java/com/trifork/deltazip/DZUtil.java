package com.trifork.deltazip;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.OutputStream;
import java.io.InputStream;

import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.FileChannel;

import java.util.Arrays;

import java.util.zip.Deflater;
// import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;
import com.jcraft.jzlib.JZlib;
import com.jcraft.jzlib.ZStream;
import com.jcraft.jzlib.ZInputStream;
import com.jcraft.jzlib.ZOutputStream;
import java.util.zip.Adler32;

public abstract class DZUtil {

    public static class ByteArrayAccess implements DeltaZip.Access {
        private final byte[] data;

		public ByteArrayAccess(byte[] data) {this.data = data;}

		public long getSize() {return data.length;}

		public ByteBuffer getRawData() {
			return ByteBuffer.wrap(data).asReadOnlyBuffer();
		}

		public ByteBuffer pread(long pos, int len) throws IOException {
			if (pos < 0 || pos > data.length) throw new IOException("Bad position");
// 			System.err.println("DB| pread("+pos+","+len+") of "+data.length);
			return ByteBuffer.wrap(data, (int)pos, len).slice().asReadOnlyBuffer();
		}
	
		public byte[] applyAppendSpec(DeltaZip.AppendSpecification spec) {
			int pos = (int) spec.prefix_size;
			ByteBuffer tail = spec.new_tail;
			int total_length = (int) (spec.prefix_size + tail.remaining());
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream(total_length);
				baos.write(data, 0, pos);

				WritableByteChannel channel = Channels.newChannel(baos);
				channel.write(tail);
				channel.close();
				return baos.toByteArray();
			} catch (IOException ioe) {throw new RuntimeException(ioe);}
		}
	}


	public static class FileAccess implements DeltaZip.Access {
		private final FileChannel file;

		public FileAccess(File f) throws IOException {
			this.file = new RandomAccessFile(f, "r").getChannel();
		}

		public FileAccess(File f, boolean write) throws IOException {
			String mode = write? "rw":"r";
			this.file = new RandomAccessFile(f, mode).getChannel();
		}

		public void close() throws IOException {
			file.close();
		}

		public long getSize() throws IOException {
			return file.size();
		}

		public ByteBuffer pread(long pos, int len) throws IOException {
// 			System.err.println("DB| pread("+pos+","+len+") of "+file);
			ByteBuffer res = ByteBuffer.allocate(len);
			while (res.hasRemaining()) {
				int r  = file.read(res, pos);
				if (r<0) throw new IOException("End of file reached");
				pos += r;
			}
			
			return res;
		}
		
		public void applyAppendSpec(DeltaZip.AppendSpecification spec) throws IOException {
			long pos = spec.prefix_size;
			ByteBuffer tail = spec.new_tail;
			long total_length = spec.prefix_size + tail.remaining();

			while (tail.hasRemaining()) {
				int w = file.write(tail, pos);
				pos += w;
			}
			if (pos != total_length) throw new IOException("Internal error");
			file.truncate(pos);
		}

	}

	//==================== ByteBuffer utilities ====================

	public static void writeBufferTo(ByteBuffer data, OutputStream out) throws IOException {
		WritableByteChannel channel = Channels.newChannel(out);
		while (data.hasRemaining()) channel.write(data);
	}

	public static void transfer(InputStream in, OutputStream out) throws IOException {
		int n;
		byte[] buf = new byte[512];
		int total=0;
		while ((n=in.read(buf)) > 0) {out.write(buf,0,n); total += n;}
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

	public static int computeAdler32(ByteBuffer data) {
		return computeAdler32(allToByteArray(data)); // Oh, the copying.
	}
	public static int computeAdler32(byte[] data) {
		Adler32 acc = new Adler32();
		acc.update(data);
		return (int)acc.getValue();
	}


    public static void writeBytestring(OutputStream dest, byte[] value) throws IOException {
        varlen_encode(value.length, dest);
        dest.write(value);
    }

    public static byte[] readBytestring(ByteBuffer src) throws IOException {
        int len = varlen_decode(src);
        byte[] res = new byte[len];
        src.get(res);
        return res;
    }

    public static void varlen_encode(int value, OutputStream out) throws IOException{
        int shift = 0;
        while ((value >>> shift) >= 0x80) shift += 7;
        for (; shift>=0; shift -= 7) {
            byte b = (byte)((value >>> shift) & 0x7F);
            if (shift>0) b |= 0x80;
            out.write(b);
        }
    }

    public static int varlen_decode(ByteBuffer in) throws IOException {
        long acc = 0;
        int bits = 0;
        boolean more;
        do {
            int b = in.get();
            more = (b < 0);
            b &= 0x7F;
            acc = (acc << 7) | (b & 0x7F);
            bits += 7;
            if (acc > Integer.MAX_VALUE) throw new IOException("Variable-length encoded integer is too large: "+acc);
        } while (more);
        return (int) acc;
    }

    //==================== Deflate / Inflate ====================

	public static class Dictionary {
		final byte[] data;
		final int off, len;
		
		public Dictionary(byte[] data, int off, int len) {
			this.data = data;
			this.off = off;
			this.len = len;
            if (off<0 || len < 0 || off+len > data.length) throw new IllegalArgumentException("Bad dictionary slice: off="+off+" len="+len+" bytes="+data.length);
		}

		public byte[] withZeroOffset() {
			return off==0? data : Arrays.copyOfRange(data, off, off+len);
		}
	}
	
	static class MyZOutputStream extends ZOutputStream {
		public MyZOutputStream(OutputStream out, int level, boolean nowrap) {
			super(out, level, nowrap);
		}
		public void setDeflateDict(Dictionary dict) {
			checkOK(z,z.deflateSetDictionary(dict.withZeroOffset(), dict.len));
		}
		public String stats() {
			return "Stats: {in="+z.total_in+", out="+z.total_out+"}";
		}
	}

	static class MyZInputStream extends ZInputStream {
		public MyZInputStream(InputStream in, boolean nowrap) {
			super(in, nowrap);
		}
		public void setInflateDict(Dictionary dict) {
// 			DeltaZip.dump("MyZInputStream.setInflateDict(): len="+dict.len, dict.withZeroOffset());
// 			z.istate.mode = com.jcraft.jzlib.Inflate.DICT0;
			checkOK(z,z.inflateSetDictionary(dict.withZeroOffset(), dict.len));
		}
		public String stats() {
			return "Stats: {in="+z.total_in+", out="+z.total_out+"}";
		}
	}

	static void checkOK(ZStream z, int errcode) {
		if (errcode != JZlib.Z_OK) {
			throw new RuntimeException("JZlib operation failed: "+z.msg+" (errcode="+errcode+")");
		}
	}

	public static byte[] inflate(Inflater inflater, ByteBuffer src, int uncomp_length, Dictionary dict) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		inflate(inflater, src, uncomp_length, baos, dict);
		baos.close();
		return baos.toByteArray();
	}

	public static void inflate(Inflater _inflater, ByteBuffer src, int comp_length, OutputStream dst, Dictionary dict) throws IOException {
		// Apparently this goes against the grain... so we need to copy.
		ByteArrayInputStream src_str = new ByteArrayInputStream(remainingToByteArray(takeStart(src, comp_length)));
		MyZInputStream zis = new MyZInputStream(src_str, true);
		if (dict != null) zis.setInflateDict(dict);
		transfer(zis, dst);
	}

	public static byte[] deflate(Deflater deflater, ByteBuffer src, int uncomp_length, Dictionary dict) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			deflate(deflater, src, uncomp_length, baos, dict);
			baos.close();
		} catch (IOException ioe) {throw new RuntimeException(ioe);}
		return baos.toByteArray();
	}

	public static void deflate(Deflater _deflater, ByteBuffer src, int uncomp_length, OutputStream dst, Dictionary dict) throws IOException {
		MyZOutputStream zos = new MyZOutputStream(dst, JZlib.Z_BEST_COMPRESSION, true);
		if (dict != null) zos.setDeflateDict(dict);
		writeBufferTo(takeStart(src, uncomp_length), zos);
		zos.finish();
	}

	/** Create a ByteBuffer which contains the 'length' first bytes of 'org'. Advance 'org' with 'length' bytes. */
	public static ByteBuffer takeStart(ByteBuffer org, int length) {
		ByteBuffer res = org.duplicate();
		res.limit(res.position() + length);
		org.position(org.position() + length);
		return res;
	}

}

