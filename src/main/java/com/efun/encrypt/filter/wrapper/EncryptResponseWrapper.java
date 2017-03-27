package com.efun.encrypt.filter.wrapper;


import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import org.apache.log4j.Logger;

import java.io.*;

/**
 * EncryptResponseWrapper
 *
 * @author Galen
 * @since 2017/3/25
 */
public class EncryptResponseWrapper extends HttpServletResponseWrapper {

    private static String charset = "utf-8";

    //存储应用传过来的数据
    private ByteArrayOutputStream buffer = null;

    private ServletOutputStream out = null;

    private PrintWriter writer = null;

    public EncryptResponseWrapper(HttpServletResponse response) throws IOException {
        super(response);
        buffer = new ByteArrayOutputStream();
        out = new WapperedOutputStream(buffer);
        //强制指定编码utf-8
        writer = new PrintWriter(new OutputStreamWriter(buffer, charset));
        Logger.getLogger(this.getClass()).info("charset:--------"+response.getCharacterEncoding());
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        return out;
    }

    @Override
    public PrintWriter getWriter() throws UnsupportedEncodingException {
        return writer;
    }

    @Override
    public void flushBuffer() throws IOException {
        if (out != null) {
            out.flush();
        }
        if (writer != null) {
            writer.flush();
        }
    }

    @Override
    public void reset() {
        buffer.reset();
    }

    public byte[] getResponseData() throws IOException {
        flushBuffer();
        return buffer.toByteArray();
    }

    private class WapperedOutputStream extends ServletOutputStream {
        private ByteArrayOutputStream bos = null;

        public WapperedOutputStream(ByteArrayOutputStream stream) throws IOException {
            bos = stream;
        }

        @Override
        public void write(int b) throws IOException {
            bos.write(b);
        }
        @Override
        public void write(byte[] b) throws IOException {
            bos.write(b, 0, b.length);
        }

        public boolean isReady() {
            return false;
        }

        public void setWriteListener(WriteListener writeListener) {

        }
    }
}
