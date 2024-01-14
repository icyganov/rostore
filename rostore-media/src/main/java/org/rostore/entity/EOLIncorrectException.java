package org.rostore.entity;

/**
 * Exception is thrown if the value of EOL violates some
 * validation rules.
 */
public class EOLIncorrectException extends RoStoreException {

    private long incorrectEOL;

    public EOLIncorrectException (final long eol) {
        super("The EOL=\"" + eol + "\" is incorrect");
    }

    public long getIncorrectEOL() {
        return incorrectEOL;
    }
}
