package com.trifork.deltazip;

import java.io.IOException;

public class ArchiveIntegrityException extends RuntimeException {
    public ArchiveIntegrityException(String message) {
        super(message);
    }

    public ArchiveIntegrityException(Throwable t) {
        super(t);
    }
}
