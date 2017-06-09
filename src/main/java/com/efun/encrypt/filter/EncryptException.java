package com.efun.encrypt.filter;

import javax.servlet.ServletException;
import java.io.IOException;

/**
 * EncryptException
 *
 * @author Galen
 * @since 2017/6/9
 */
public class EncryptException extends ServletException {

    public EncryptException() {
        super();
    }

    public EncryptException(String message) {
        super(message);
    }

    public EncryptException(String message, Throwable cause) {
        super(message, cause);
    }

    public EncryptException(Throwable cause) {
        super(cause);
    }

}
