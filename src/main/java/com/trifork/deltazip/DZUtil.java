package com.trifork.deltazip;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.Channels;

public abstract class DZUtil {
	public static class ByteArrayAccess implements DeltaZip.Access {
		final byte[] data;
		public ByteArrayAccess(byte[] data) {this.data = data;}

		public long getSize() {return data.length;}
		public ByteBuffer pread(long pos, long len) {
			System.err.println("DB| pread("+pos+","+len+") of "+data.length);
			return ByteBuffer.wrap(data, (int)pos, (int)len).slice().asReadOnlyBuffer();
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
		
	}
}

