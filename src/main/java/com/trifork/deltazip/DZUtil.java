package com.trifork.deltazip;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.RandomAccessFile;

import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.FileChannel;

import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;
import java.util.zip.Adler32;

import java.io.OutputStream;

public abstract class DZUtil {
	public static class ByteArrayAccess implements DeltaZip.Access {
		private final byte[] data;

		public ByteArrayAccess(byte[] data) {this.data = data;}

		public long getSize() {return data.length;}

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

	//====================
	public static void writeBufferTo(ByteBuffer data, OutputStream out) throws IOException {
		WritableByteChannel channel = Channels.newChannel(out);
		while (data.hasRemaining()) channel.write(data);
	}

	public static int computeAdler32(ByteBuffer data) {
		return computeAdler32(DeltaZip.allToByteArray(data)); // Oh, the copying.
	}
	public static int computeAdler32(byte[] data) {
		Adler32 acc = new Adler32();
		acc.update(data);
		return (int)acc.getValue();
	}

	//==================== Deflate / Inflate ====================

	public static class Dictionary {
		final byte[] data;
		final int off, len;
		
		public Dictionary(byte[] data, int off, int len) {
			this.data = data;
			this.off = off;
			this.len = len;
		}
	}

	public static byte[] inflate(Inflater inflater, ByteBuffer src, int uncomp_length, Dictionary dict) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		inflate(inflater, src, uncomp_length, baos, dict);
		baos.close();
		return baos.toByteArray();
	}

	public static void inflate(Inflater inflater, ByteBuffer src, int comp_length, OutputStream dst, Dictionary dict) throws IOException {
		inflater.reset();
		InflaterOutputStream zos = new InflaterOutputStream(dst, inflater);

		if (dict != null) inflater.setDictionary(dict.data, dict.off, dict.len);

		writeBufferTo(takeStart(src, comp_length), zos);
		zos.finish();
	}

	public static byte[] deflate(Deflater deflater, ByteBuffer src, int uncomp_length, Dictionary dict) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try {
			deflate(deflater, src, uncomp_length, baos, dict);
			baos.close();
		} catch (IOException ioe) {throw new RuntimeException(ioe);}
		return baos.toByteArray();
	}

	public static void deflate(Deflater deflater, ByteBuffer src, int uncomp_length, OutputStream dst, Dictionary dict) throws IOException {
		deflater.reset();
		DeflaterOutputStream zos = new DeflaterOutputStream(dst, deflater);

		if (dict != null) deflater.setDictionary(dict.data, dict.off, dict.len);

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

