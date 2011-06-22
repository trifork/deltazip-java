package com.trifork.deltazip;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.RandomAccessFile;

import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.FileChannel;

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
			try {
				int length = (int) (spec.prefix_size + spec.new_tail.remaining());
				ByteArrayOutputStream baos = new ByteArrayOutputStream(length);
				baos.write(data);
				WritableByteChannel channel = Channels.newChannel(baos);
				channel.write(spec.new_tail);
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
			long total_length = (spec.prefix_size + tail.remaining());

			while (tail.hasRemaining()) {
				int w = file.write(tail, pos);
				pos += w;
			}
			if (pos != total_length) throw new IOException("Internal error");
			file.truncate(pos);
		}

	}
}

