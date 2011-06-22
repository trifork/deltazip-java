package com.trifork.deltazip;

import com.trifork.deltazip.DZUtil.FileAccess;
import com.trifork.deltazip.DeltaZip.AppendSpecification;
import java.io.File;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

public abstract class DeltaZipCLI {

	public static void main(String[] args) throws IOException {
		if (args.length < 1) {usage(); System.exit(1);}

		String command = args[0];
		if ("count".equals(command)) do_count(args);
		else if ("get".equals(command)) do_get(args);
		else if ("create".equals(command)) do_create(args);
		else if ("add".equals(command)) do_add(args);
		else {usage(); System.exit(1);}
	}

	public static void usage() {
		System.err.println("Usage: deltazip [COMMAND] [ARGS]");
		System.err.println("Commands:");
		System.err.println("  create [dzfile] [version-files]");
		System.err.println("  get  [dzfile]    Print the last version");
		System.err.println("  get @n [dzfile]  Print the nth-last version");
		System.err.println("  count [dzfile]   Count the number of versions");
		System.err.println("  add [dzfile] [version-files]");
	}

	public static void do_count(String[] args) throws IOException {
		if (args.length != 2) {usage(); System.exit(1);}
		FileAccess fa = openDZFile(args[1]);
		DeltaZip dz = new DeltaZip(fa);

		int count = 0;
		if (dz.get() != null) {
			count++;
			while (dz.hasPrevious()) {
				dz.previous();
				count++;
			}
		}
		fa.close();
		System.out.println(count);
	}

	public static void do_get(String[] args) throws IOException {
		if (args.length < 2) {usage(); System.exit(1);}
		int rev_nr = 0;
		int file_arg = 1;
		if (args[file_arg].startsWith("@")) {
			rev_nr = Integer.parseInt(args[file_arg].substring(1));
			file_arg++;
		}
		if (args.length != file_arg+1) {usage(); System.exit(1);}

		FileAccess fa = openDZFile(args[file_arg]);
		DeltaZip dz = new DeltaZip(fa);

		if (dz.get() == null) {
			System.err.println("Archive is empty.");
			System.exit(3);
		} else {
			for (int i=0; i<rev_nr; i++) {
				if (! dz.hasPrevious()) {
					System.err.println("Archive only contains "+(i+1)+" versions.");
					System.exit(3);
				}
				dz.previous();
			}
			System.out.write(DeltaZip.allToByteArray(dz.get()));
		}
		fa.close();
	}

	public static void do_create(final String[] args) throws IOException {
		if (args.length < 2) {usage(); System.exit(1);}
		String filename = args[1];
		File dzfile = new File(filename);
		if (dzfile.exists()) {
			System.err.println("File already exists: "+filename);
			System.exit(2);
		}

		// Possible race condition here. Can't do anything about it I think.
		FileAccess fa = new FileAccess(dzfile, true);
		add_to_file(fa, createFileIterator(args, 2));
		fa.close();
	}

	public static void do_add(String[] args) throws IOException {
		if (args.length < 2) {usage(); System.exit(1);}
		String filename = args[1];
		FileAccess fa = openDZFile(filename, true, false);
		add_to_file(fa, createFileIterator(args, 2));
		fa.close();
	}

	protected static void add_to_file(FileAccess fa, Iterator<ByteBuffer> to_add) throws IOException {
		DeltaZip dz = new DeltaZip(fa);
		AppendSpecification app_spec = dz.add(to_add);
		fa.applyAppendSpec(app_spec);
	}


	//======================================================================
	private static FileAccess openDZFile(String filename) throws IOException {
		return openDZFile(filename, false, false);
	}

	private static FileAccess openDZFile(String filename, boolean writable, boolean create) throws IOException {
		File dzfile = new File(filename);
		if (!create) checkExistence(dzfile);
		return new FileAccess(dzfile, writable);
	}

	private static void checkExistence(File file) {
		if (! file.exists()) {
			System.err.println("No such file: "+file);
			System.exit(2);
		}
	}

	private static ByteBuffer fileToByteBuffer(String filename) throws IOException {
		FileInputStream in = new FileInputStream(filename);
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			byte[] tmp = new byte[4096];
			int r;
			while ((r = in.read(tmp)) >= 0) {
				baos.write(tmp, 0, r);
			}
			return ByteBuffer.wrap(baos.toByteArray());
		} finally {
			in.close();
		}
	}

	protected static Iterator<ByteBuffer> createFileIterator(final String[] filenames, final int start_index) {
		return new Iterator<ByteBuffer>() {
			int pos = start_index;

			public boolean hasNext() {return pos < filenames.length;}

			public ByteBuffer next() {
				String filename = filenames[pos++];
				try {
					return fileToByteBuffer(filename);
				} catch (IOException ioe) {
					throw new RuntimeException(ioe);
				}
			}
			public void remove() {throw new UnsupportedOperationException();}
		};
	}

}