package de.angelasensio.sftrino;

/**
 * Custom exception for SQL translation errors.
 */
public class SqlTranslationException extends Exception {
    
    public SqlTranslationException(String message) {
        super(message);
    }
    
    public SqlTranslationException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public SqlTranslationException(Throwable cause) {
        super(cause);
    }
}