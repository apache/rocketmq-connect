package org.apache.rocketmq.connect.http.sink.auth;

import com.google.common.base.Strings;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.concurrent.Callable;

import static org.apache.rocketmq.connect.http.sink.constant.HttpConstant.LOG_SIFT_TAG;


public class HttpRequestCallable implements Callable<String> {

    private static final Logger log = LoggerFactory.getLogger(HttpCallback.class);

    private CloseableHttpClient httpclient;

    private HttpCallback httpCallback;

    private HttpUriRequest httpUriRequest;

    private HttpClientContext context;
    private SocksProxyConfig socksProxyConfig;
    private String siftTag;

    private static final String SOCKS_ADDRESS_KEY = "socks.address";

    public HttpRequestCallable(CloseableHttpClient httpclient, HttpUriRequest httpUriRequest, HttpClientContext context,
                               SocksProxyConfig socksProxyConfig, HttpCallback httpCallback, String siftTag) {
        this.httpclient = httpclient;
        this.httpUriRequest = httpUriRequest;
        this.context = context;
        this.socksProxyConfig = socksProxyConfig;
        this.httpCallback = httpCallback;
        this.siftTag = siftTag;
    }

    public void loadSocks5ProxyConfig() {
        if (!Strings.isNullOrEmpty(socksProxyConfig.getSocks5Endpoint())) {
            String[] socksAddrAndPor = socksProxyConfig.getSocks5Endpoint()
                    .split(":");
            InetSocketAddress socksaddr = new InetSocketAddress(socksAddrAndPor[0],
                    Integer.parseInt(socksAddrAndPor[1]));
            context.setAttribute(SOCKS_ADDRESS_KEY, socksaddr);
            ThreadLocalProxyAuthenticator.getInstance()
                    .setCredentials(socksProxyConfig.getSocks5UserName(), socksProxyConfig.getSocks5Password());
        }
    }

    @Override
    public String call() throws Exception {
        MDC.put(LOG_SIFT_TAG, siftTag);
        CloseableHttpResponse response = null;
        try {
            Long startTime = System.currentTimeMillis();
            loadSocks5ProxyConfig();
            response = httpclient.execute(httpUriRequest, context);
            if (response.getStatusLine()
                    .getStatusCode() / 100 != 2) {
                String msg = MessageFormat.format("Http Status:{0},Msg:{1}", response.getStatusLine()
                        .getStatusCode(), EntityUtils.toString(response.getEntity()));
                httpCallback.setMsg(msg);
                httpCallback.setFailed(Boolean.TRUE);
            }
            log.info("The cost of one http request:{}， Connection Connection={},Keep-Alive={}",
                    System.currentTimeMillis() - startTime, response.getHeaders("Connection"),
                    response.getHeaders("Keep-Alive"));
            return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
        } catch (Throwable e) {
            log.error("http execute failed.", e);
            httpCallback.setFailed(Boolean.TRUE);
            httpCallback.setMsg(e.getLocalizedMessage());
        } finally {
            httpCallback.getCountDownLatch()
                    .countDown();
            if (null != response.getEntity()) {
                EntityUtils.consume(response.getEntity());
            }
        }
        return null;
    }
}
