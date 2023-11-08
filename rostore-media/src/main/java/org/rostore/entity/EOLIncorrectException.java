package org.rostore.entity;

public class EOLIncorrectException extends RuntimeException {

    private long incorrectEOL;

    public EOLIncorrectException (final long eol) {
        super("The EOL=\"" + eol + "\" is incorrect");
    }

    public long getIncorrectEOL() {
        return incorrectEOL;
    }
}
