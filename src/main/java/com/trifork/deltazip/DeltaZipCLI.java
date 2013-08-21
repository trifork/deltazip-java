package com.trifork.deltazip;

import com.trifork.deltazip.DZUtil.FileAccess;
import com.trifork.deltazip.DeltaZip.AppendSpecification;
import java.io.File;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

public abstract class DeltaZipCLI {

	public static void main(String[] args) throws IOException {
		if (args.length < 1) {usage(); System.exit(1);}

		String command = args[0];
		if ("count".equals(command))       do_count(args);
		else if ("list".equals(command))   do_list(args);
		else if ("get".equals(command))    do_get(args);
		else if ("create".equals(command)) do_create(args);
		else if ("add".equals(command))    do_add(args);
		else {usage(); System.exit(1);}
	}

	public static void usage() {
		System.err.println("Usage: deltazip [COMMAND] [ARGS]");
		System.err.println("Commands:");
		System.err.println("  create [dzfile] [version-files]");
		System.err.println("  get  [dzfile]    Print the last version");
		System.err.println("  get @n [dzfile]  Print the nth-last version");
		System.err.println("  count [dzfile]   Count the number of versions");
		System.err.println("  list [dzfile]   List versions and their statistics");
		System.err.println("  add [dzfile] [version-files]");
	}

	//====================
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

	//====================
	public static void do_list(String[] args) throws IOException {
		if (args.length != 2) {usage(); System.exit(1);}
		FileAccess fa = openDZFile(args[1]);
		DeltaZip dz = new DeltaZip(fa);

		System.out.println("Nr:\tMethod\tCompSize\tVersionSize\tChecksum\tMetadata");

		if (dz.get() == null) return;

		int nr = 0;
		for (;; nr++) {
			String line =
				String.format("%d:\t"+"M%d\t"+"%8d\t"+"%8d\t"+"%8x\t%s",
							  (-nr),
							  dz.getCurrentMethod(),
							  dz.getCurrentCompSize(),
							  dz.getCurrentRawSize(),
							  dz.getCurrentChecksum(),
                              metadataToString(dz.getMetadata()));
			System.out.println(line);
			
			if (dz.hasPrevious()) {
				dz.previous();
			} else {
				break;
			}
		}
		fa.close();
	}

    private static String metadataToString(List<Metadata.Item> metadata) {
        StringBuilder sb = new StringBuilder();
        SimpleDateFormat dfmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");//spec for RFC3339
        dfmt.setTimeZone(TimeZone.getTimeZone("UTC"));
        for (Metadata.Item item : metadata) {
            if (sb.length() > 0) sb.append("; ");
            int keytag = item.getNumericKeytag();
            String key_str = Metadata.keytag_to_name(keytag);
            if (key_str==null) key_str = String.valueOf(keytag);

            String value_str;
            if (item instanceof Metadata.Timestamp) {
                key_str = "timestamp";
                value_str = dfmt.format(((Metadata.Timestamp)item).getDate());
            } else {
                value_str = new String(item.getValue(), Metadata.UTF8);
            }

            sb.append(key_str).append("=\"").append(value_str).append('"');
        }
        return sb.toString();
    }

    //====================
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
			System.out.write(DZUtil.allToByteArray(dz.get()));
		}
		fa.close();
	}

	//====================
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

	//====================
	public static void do_add(String[] args) throws IOException {
		if (args.length < 2) {usage(); System.exit(1);}
		String filename = args[1];
		FileAccess fa = openDZFile(filename, true, false);
		add_to_file(fa, createFileIterator(args, 2));
		fa.close();
	}

	protected static void add_to_file(FileAccess fa, Iterator<Version> to_add) throws IOException {
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

	protected static Iterator<Version> createFileIterator(final String[] args, final int start_index) {
		return new Iterator<Version>() {
			int pos = start_index;
            List<Metadata.Item> metadata_for_next_filename = eatMetadata();

            private List<Metadata.Item> eatMetadata() {
                List<Metadata.Item> res = new ArrayList<Metadata.Item>();
                while (pos < args.length && args[pos].startsWith("-m")) {
                    String md_spec = args[pos++];
                    int eq_pos = md_spec.indexOf("=");
                    if (eq_pos < 0) {
                        throw new IllegalArgumentException("Metadata specification contains no \"=\" sign");
                    }
                    String md_key = md_spec.substring(2,eq_pos);
                    String md_value = md_spec.substring(eq_pos+1);
                    res.add(cli_arg_to_metadata_item(md_key, md_value));
                }
                if (pos >= args.length && !res.isEmpty()) {
                    throw new IllegalArgumentException("Parameter list ends with metadata, not with a version filename");
                }
                return res;
            }

            public boolean hasNext() {return pos < args.length;}

			public Version next() {
				String filename = args[pos++];
                try {
                    return new Version(fileToByteBuffer(filename), metadata_for_next_filename);
                } catch (IOException ioe) {
                    throw new RuntimeException(ioe);
                } finally {
                    metadata_for_next_filename = eatMetadata();
                }
            }
			public void remove() {throw new UnsupportedOperationException();}
		};
	}

    private static Metadata.Item cli_arg_to_metadata_item(String key_str, String value_str) {
        Integer keytag_opt = Metadata.name_to_keytag(key_str);
        final int keytag;
        if (keytag_opt != null) { // Named keytag.
            keytag = keytag_opt.intValue();
            if (keytag == Metadata.TIMESTAMP_KEYTAG) {
                SimpleDateFormat dfmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");//spec for RFC3339
                dfmt.setLenient(true);
                dfmt.setTimeZone(TimeZone.getTimeZone("UTC"));
                Date d;
                try {
                    d = dfmt.parse(value_str);
                } catch (ParseException e) {
                    throw new IllegalArgumentException("Bad metadata timestamp format: \""+value_str+"\"");
                }
                return new Metadata.Timestamp(d);
            } else {
                return new Metadata.Item(keytag, value_str);
            }
        } else { // Numeric keytag?
            try {
                keytag = Integer.parseInt(key_str);
            } catch (NumberFormatException nfe) {
                throw new IllegalArgumentException("Unrecognized metadata key: \""+key_str+"\"");
            }
            return new Metadata.Item(keytag, value_str);
        }
    }

}
