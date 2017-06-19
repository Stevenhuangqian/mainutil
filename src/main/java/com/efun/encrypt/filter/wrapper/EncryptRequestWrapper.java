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
import java.util.*;

/**
 * EncryptRequestWrapper
 *
 * @author Galen
 * @since 2017/3/25
 */
public class EncryptRequestWrapper extends HttpServletRequestWrapper {

    private Map<String, String[]> params;

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
    private Map<String, String[]> createParams(String queryString, String bodyString) {
        Map params = new HashMap<String, Object>();
        if (StringUtils.isNotBlank(queryString)) {
            params.putAll(parseForm(queryString));
        }
        if (StringUtils.isNotBlank(bodyString)) {
            params.putAll(parseForm(bodyString));
        }
        return params;
    }

    private Map<String, String[]> parseForm(String form) {
        Map<String, String[]> params = new HashMap<String, String[]>();
        if (form.trim().length() > 0) {
            Map<String, List<String>> map = new HashMap<String, List<String>>();
            String[] kvAarry = form.split("&");
            for (String item : kvAarry) {
                int index = item.indexOf("=");
                if (index > 0) {
                    String key = item.substring(0, index).trim();
                    String value = item.substring(index + 1).trim();
                    List<String> values = map.get(key);
                    if (values == null) {
                        values = new ArrayList<String>();
                    }
                    values.add(value);
                    map.put(key, values);
                } else {
                    String key = item.trim();
                    map.put(key, null);
                }
            }
            Set<Map.Entry<String, List<String>>> entrySet = map.entrySet();
            for (Map.Entry<String, List<String>> item : entrySet) {
                if (item.getValue() == null) {
                    params.put(item.getKey(), new String[1]);
                } else {
                    params.put(item.getKey(), item.getValue().toArray(new String[item.getValue().size()]));
                }
            }
        }
        return params;
    }

    @Override
    public String getQueryString() {
        return this.queryString;
    }

    @Override
    public String getParameter(String name) {
        String[] s = this.params.get(name);
        return s != null && s.length >0 ? s[0] : null;
    }

    @Override
    public Map<String, String[]> getParameterMap() {
        return this.params;
    }

    @Override
    public Enumeration<String> getParameterNames() {
    	Vector l = new Vector(params.keySet());
		return l.elements();
    }

    @Override
    public String[] getParameterValues(String name) {
        return this.params.get(name);
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
