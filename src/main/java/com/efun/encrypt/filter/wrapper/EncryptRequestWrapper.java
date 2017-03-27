package com.efun.encrypt.filter.wrapper;


import org.apache.commons.lang3.StringUtils;

import javax.servlet.ReadListener;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

/**
 * EncryptRequestWrapper
 *
 * @author Galen
 * @since 2017/3/25
 */
public class EncryptRequestWrapper extends HttpServletRequestWrapper {

    private Map params;

    private String queryString;

    private ServletInputStream inputStream;

    private BufferedReader reader;

    public EncryptRequestWrapper(HttpServletRequest request) {
        super(request);
    }

    public EncryptRequestWrapper(HttpServletRequest request, String queryString, String bodyString, InputStream inputStream) {
        super(request);
        this.queryString = queryString;
        this.params = createParams(queryString, bodyString);
        this.inputStream = new EncryptInputStream(inputStream);
        this.reader = new BufferedReader(new InputStreamReader(this.inputStream));

    }

    /**
     * 生成参数params
     * @param queryString
     * @param bodyString
     * @return
     */
    private Map createParams(String queryString, String bodyString) {
        Map params = new HashMap<String, Object>();
        if (StringUtils.isNotBlank(queryString)) {
            params.putAll(parseForm(queryString));
        }
        if (StringUtils.isNotBlank(bodyString)) {
            params.putAll(parseForm(bodyString));
        }
        return params;
    }

    private Map parseForm(String form) {
        Map map = new HashMap<String, Object>();
        if (form.trim().length() > 0) {
            String[] kvAarry = form.split("&");
            for (String item : kvAarry) {
                String[] kv = item.split("=");
                if (kv.length > 1) {
                    map.put(kv[0], kv[1]);
                }else{
                	map.put(kv[0], "");
                }
            }
        }
        return map;
    }

    @Override
    public String getQueryString() {
        return this.queryString;
    }

    @Override
    public String getParameter(String name) {
        return (String) this.params.get(name);
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        return (Map<String, String[]>)this.params;
    }

    @Override
    public Enumeration<String> getParameterNames() {
    	Vector l = new Vector(params.keySet());
		return l.elements();
    }

    @Override
    public String[] getParameterValues(String name) {
        return (String[]) this.params.values().toArray();
    }

    @Override
    public ServletInputStream getInputStream() throws IOException {
        return this.inputStream;
    }

    @Override
    public BufferedReader getReader() throws IOException {
        return this.reader;
    }

    public class EncryptInputStream extends ServletInputStream {

        InputStream _is = null;

        public EncryptInputStream(InputStream sourceInputStream) {
            _is = sourceInputStream;
        }

        public int available() throws IOException {
            return this._is == null?-1:this._is.available();
        }

        public int read() throws IOException {
            return this._is == null?-1:this._is.read();
        }

        public int read(byte[] buf, int offset, int len) throws IOException {
            return this._is == null?-1:this._is.read(buf, offset, len);
        }

        public long skip(long n) throws IOException {
            return this._is == null?-1L:this._is.skip(n);
        }

        public void close() throws IOException {
        }

        public void free() {
            this._is = null;
        }

        public String toString() {
            return this.getClass().getSimpleName() + "[" + this._is + "]";
        }

        public boolean isFinished() {
            return false;
        }

        public boolean isReady() {
            return false;
        }

        public void setReadListener(ReadListener readListener) {

        }
    }
}
