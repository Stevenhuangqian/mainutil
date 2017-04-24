package com.efun.encrypt.filter;

import com.efun.encrypt.filter.algorithm.AESUtils;
import com.efun.encrypt.filter.algorithm.MD5Utils;
import com.efun.encrypt.filter.algorithm.RSAUtils;
import com.efun.encrypt.filter.wrapper.EncryptRequestWrapper;
import com.efun.encrypt.filter.wrapper.EncryptResponseWrapper;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.security.interfaces.RSAPrivateKey;
import java.util.Base64;

/**
 * EncryptFilter
 *
 * @author Galen
 * @since 2017/3/25
 */
public class EncryptFilter implements Filter {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static String charset = "utf-8";

    //同时兼容两种header
    private static final String REQUEST_KEY = "requestID";

    private static final String EFUN_REQUEST_KEY = "efunRequestID";

    private static final String RESPONSE_KEY = "responseID";

    private static final String EFUN_RESPONSE_KEY = "efunResponseID";

    // TODO: 2017/3/25  私钥（暂时hardcode到代码中），应该转用配置方式。
    private RSAPrivateKey private_key;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {

    }

    @Override  
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;
        response.setCharacterEncoding("utf-8");
        //透传编码
        charset = StringUtils.isNotBlank(request.getCharacterEncoding()) ? request.getCharacterEncoding() : charset;

        //获取RSA加密后的AES密钥
        String requestId = request.getHeader(EFUN_REQUEST_KEY);
        boolean isEfunRequest = true;
        if (StringUtils.isBlank(requestId)) {
            requestId = request.getHeader(REQUEST_KEY);
            isEfunRequest = false;
        }

        //判断内容是否有经过加密
        if (StringUtils.isNotBlank(requestId)) {

            //解密request
            EncryptRequestWrapper requestWrapper = decryptRequest(requestId, request);

            EncryptResponseWrapper responseWrapper = new EncryptResponseWrapper(response);
            //进入调用链
            filterChain.doFilter(requestWrapper, responseWrapper);

            //加密response
            encryptResponse(responseWrapper, response, isEfunRequest);


        } else {
            filterChain.doFilter(servletRequest, servletResponse);
        }

    }

    /**
     * 解密输入内容
     * @return
     */
    private EncryptRequestWrapper decryptRequest(String requestId, HttpServletRequest request) {
        //获取AES解密使用的key和iv,网络交互的信息都统一用base64转码过。
        byte[] key_iv = decryptAESKey(Base64.getDecoder().decode(requestId));
        byte[] key = new byte[16];
        byte[] iv = new byte[16];
        System.arraycopy(key_iv, 0, key, 0, 16);
        System.arraycopy(key_iv, 16, iv, 0, 16);
        String queryString = null;
        String bodyString = null;
        InputStream inputStream = null;
        String method = request.getMethod();
        try {
            //提取数据，重新包装一个新的inputStream对象。
            if ("get".equalsIgnoreCase(method)) {
                queryString = new String(AESUtils.decrypt(Base64.getDecoder().decode(request.getQueryString()), key, iv), charset);
                queryString = URLDecoder.decode(queryString);

            } else if ("post".equalsIgnoreCase(method)) {
                byte[] data = getRequestPostBytes(request.getInputStream());
                data = Base64.getDecoder().decode(data);
                data = AESUtils.decrypt(data, key, iv);
                bodyString = URLDecoder.decode(new String(data, charset));
                inputStream = new ByteArrayInputStream(URLDecoder.decode(bodyString).getBytes(charset));
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return new EncryptRequestWrapper(request, queryString, bodyString, inputStream);
    }

    /**
     * 加密返回内容
     */
    private void encryptResponse(EncryptResponseWrapper wrapper, HttpServletResponse response, boolean isEfunResponse) {
        try {
            byte[] data = wrapper.getResponseData();
            String responseId = MD5Utils.MD5(new String(data, charset));
            byte[] key_iv = responseId.getBytes(charset);
            byte[] key = new byte[16];
            byte[] iv = new byte[16];
            System.arraycopy(key_iv, 0, key, 0, 16);
            System.arraycopy(key_iv, 16, iv, 0, 16);
            data = Base64.getEncoder().encode(AESUtils.encrypt(data, key, iv));
            responseId = new String(Base64.getEncoder().encode(encryptAESKey(responseId.getBytes(charset))), charset);
            if (isEfunResponse) {
                response.addHeader(EFUN_RESPONSE_KEY, responseId);
            } else {
                response.addHeader(RESPONSE_KEY, responseId);
            }
            response.setContentLength(data.length);
            response.getOutputStream().write(data);
            response.getOutputStream().flush();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * 使用RSA加密，获取加密后AES的key
     * @param bytes
     * @return
     */
    private byte[] encryptAESKey(byte[] bytes) {
        try {
            return RSAUtils.encrypt(bytes);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    /**
     * 使用RSA解密，获取解密后AES的key
     * @param bytes
     * @return
     */
    private byte[] decryptAESKey(byte[] bytes) {
        try {
            return RSAUtils.decrypt(bytes);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return null;
    }

    private byte[] getRequestPostBytes(InputStream inputStream)
            throws IOException {
        ByteArrayOutputStream swapStream = new ByteArrayOutputStream();
        byte[] buff = new byte[100];
        int rc = 0;
        while ((rc = inputStream.read(buff, 0, 100)) > 0) {
            swapStream.write(buff, 0, rc);
        }
        byte[] bytes = swapStream.toByteArray();
        return bytes;
    }

    @Override
    public void destroy() {

    }

}
