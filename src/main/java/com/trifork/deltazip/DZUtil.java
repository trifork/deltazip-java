package com.trifork.deltazip;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.RandomAccessFile;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.Channels;

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


	public static class FileAccess {
		private final RandomAccessFile file;

		public FileAccess(File f) throws IOException {
			this.file = new RandomAccessFile(f, "r");
		}

		public long getSize() throws IOException {
			return file.length();
		}

		public ByteBuffer pread(long pos, int len) throws IOException {
// 			System.err.println("DB| pread("+pos+","+len+") of "+file);
			byte[] buf = new byte [len];
			file.seek(pos);
			file.read(buf);
			
			return ByteBuffer.wrap(buf).asReadOnlyBuffer();
		}
		
	}
}

