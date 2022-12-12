package org.apache.rocketmq.connect.http.sink.auth;

import com.google.common.net.MediaType;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.DnsResolver;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.rocketmq.connect.http.sink.entity.ClientConfig;
import org.apache.rocketmq.connect.http.sink.entity.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.rocketmq.connect.http.sink.constant.HttpConstant.LOG_SIFT_TAG;


public class ApacheHttpClientImpl implements AbstractHttpClient {
    private static final Logger log = LoggerFactory.getLogger(ApacheHttpClientImpl.class);

    private static ExecutorService executorServicePool = new ThreadPoolExecutor(200, 2000, 600, TimeUnit.SECONDS,
            new LinkedBlockingDeque<Runnable>(1000), new DefaultThreadFactory("ApacheHttpClientRequestThread"));
    private CloseableHttpClient httpClient = null;

    private SocksProxyConfig socksProxyConfig;
    private static final String SOCKS_ADDRESS_KEY = "socks.address";

    @Override
    public void init(ClientConfig config) {
        try {
            SSLContextBuilder sslContextBuilder = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                @Override
                public boolean isTrusted(X509Certificate[] chain, String authType) {
                    return true;
                }
            });
            Registry<ConnectionSocketFactory> reg = RegistryBuilder.<ConnectionSocketFactory>create().register("http",
                            new SocksPlainConnectionSocketFactory())
                    .register("https", new SocksSSLConnectionSocketFactory(sslContextBuilder.build()))
                    .build();
            PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(reg,
                    new FakeDnsResolver());
            connManager.setMaxTotal(400);
            connManager.setDefaultMaxPerRoute(500);
            httpClient = HttpClients.custom()
                    .setConnectionManager(connManager)
                    .build();

            this.socksProxyConfig = new SocksProxyConfig(config.getProxyHost(), config.getProxyUser(), config.getProxyPassword());
        } catch (Exception e) {
            log.error("ApacheHttpClientImpl | init | error => ", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String execute(HttpRequest httpRequest, HttpCallback httpCallback) throws Exception {
        CloseableHttpResponse response;
        HttpRequestBase httpRequestBase = null;
        if (httpRequest != null) {
            httpRequestBase = extracted(httpRequest.getUrl(), httpRequest.getMethod(), httpRequest.getHeaderMap(), httpRequest.getBody());
            if (StringUtils.isNotBlank(httpRequest.getTimeout())) {
                final RequestConfig requestConfig = RequestConfig.custom().
                        setConnectionRequestTimeout(Integer.parseInt(httpRequest.getTimeout())).
                        setSocketTimeout(Integer.parseInt(httpRequest.getTimeout())).
                        setConnectTimeout(Integer.parseInt(httpRequest.getTimeout())).build();
                httpRequestBase.setConfig(requestConfig);
            }
        }
        HttpRequestCallable httpRequestCallable = new HttpRequestCallable(httpClient, httpRequestBase,
                HttpClientContext.create(), this.socksProxyConfig, httpCallback, MDC.get(LOG_SIFT_TAG));
        Future<String> submit = executorServicePool.submit(httpRequestCallable);
        String result = submit.get();
        log.info("ApacheHttpClientImpl | execute｜ success | result : {}", result);
        return result;
    }

    private HttpRequestBase extracted(String url, String method, Map<String, String> headerMap, String body) throws UnsupportedEncodingException {
        switch (method) {
            case "GET":
                HttpGet httpGet = new HttpGet(url);
                headerMap.forEach(httpGet::addHeader);
                httpGet.addHeader(HTTP.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
                return httpGet;
            case "POST":
                HttpPost httpPost = new HttpPost(url);
                headerMap.forEach(httpPost::addHeader);
                httpPost.addHeader(HTTP.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
                if (StringUtils.isNotBlank(body)) {
                    HttpEntity entityPot = new StringEntity(body);
                    httpPost.setEntity(entityPot);
                }
                return httpPost;
            case "DELETE":
                HttpDelete httpDelete = new HttpDelete(url);
                headerMap.forEach(httpDelete::addHeader);
                httpDelete.addHeader(HTTP.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
                return httpDelete;
            case "PUT":
                HttpPut httpPut = new HttpPut(url);
                headerMap.forEach(httpPut::addHeader);
                httpPut.addHeader(HTTP.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
                if (StringUtils.isNotBlank(body)) {
                    HttpEntity entityPot = new StringEntity(body);
                    httpPut.setEntity(entityPot);
                }
                return httpPut;
            case "HEAD":
                HttpHead httpHead = new HttpHead(url);
                headerMap.forEach(httpHead::addHeader);
                httpHead.addHeader(HTTP.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
                return httpHead;
            case "TRACE":
                HttpTrace httpTrace = new HttpTrace(url);
                headerMap.forEach(httpTrace::addHeader);
                httpTrace.addHeader(HTTP.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
                break;
            case "PATCH":
                HttpPatch httpPatch = new HttpPatch(url);
                headerMap.forEach(httpPatch::addHeader);
                httpPatch.addHeader(HTTP.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
                if (StringUtils.isNotBlank(body)) {
                    HttpEntity entityPot = new StringEntity(body);
                    httpPatch.setEntity(entityPot);
                }
                return httpPatch;
            default:
        }
        HttpOptions httpOptions = new HttpOptions(url);
        headerMap.forEach(httpOptions::addHeader);
        httpOptions.addHeader(HTTP.CONTENT_TYPE, MediaType.JSON_UTF_8.toString());
        return httpOptions;
    }

    @Override
    public void close() {
        try {
            httpClient.close();
        } catch (IOException e) {
            log.error("ApacheHttpClientImpl | close | error => ", e);
        }
    }

    /**
     * 实现 http 链接的socket 工厂
     */
    static class SocksPlainConnectionSocketFactory extends PlainConnectionSocketFactory {

        @Override
        public Socket createSocket(final HttpContext context) throws IOException {
            InetSocketAddress socksaddr = (InetSocketAddress)context.getAttribute(SOCKS_ADDRESS_KEY);
            if (socksaddr != null) {
                Proxy proxy = new Proxy(Proxy.Type.SOCKS, socksaddr);
                return new Socket(proxy);
            } else {
                return new Socket();
            }
        }

        @Override
        public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host, InetSocketAddress remoteAddress,
                                    InetSocketAddress localAddress, HttpContext context) throws IOException {
            InetSocketAddress socksaddr = (InetSocketAddress)context.getAttribute(SOCKS_ADDRESS_KEY);
            if (socksaddr != null) {//make proxy server to resolve host in http url
                remoteAddress = InetSocketAddress.createUnresolved(host.getHostName(), host.getPort());
            }
            return super.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
        }
    }

    static class FakeDnsResolver implements DnsResolver {
        @Override
        public InetAddress[] resolve(String host) throws UnknownHostException {
            // Return some fake DNS record for every request, we won't be using it
            try {
                return new InetAddress[] {InetAddress.getByName(host)};
            } catch (Throwable e) {
                return new InetAddress[] {InetAddress.getByAddress(new byte[] {0, 0, 0, 0})};
            }
        }
    }

    /**
     * 实现 https 链接的socket 工厂
     */
    static class SocksSSLConnectionSocketFactory extends SSLConnectionSocketFactory {
        public SocksSSLConnectionSocketFactory(SSLContext sslContext) {
            super(sslContext, NoopHostnameVerifier.INSTANCE);
        }

        @Override
        public Socket createSocket(final HttpContext context) throws IOException {
            InetSocketAddress socksaddr = (InetSocketAddress)context.getAttribute(SOCKS_ADDRESS_KEY);
            if (socksaddr != null) {
                Proxy proxy = new Proxy(Proxy.Type.SOCKS, socksaddr);
                return new Socket(proxy);
            } else {
                return new Socket();
            }
        }

        @Override
        public Socket connectSocket(int connectTimeout, Socket socket, HttpHost host, InetSocketAddress remoteAddress,
                                    InetSocketAddress localAddress, HttpContext context) throws IOException {
            InetSocketAddress socksaddr = (InetSocketAddress)context.getAttribute(SOCKS_ADDRESS_KEY);
            if (socksaddr != null) {
                remoteAddress = InetSocketAddress.createUnresolved(host.getHostName(), host.getPort());
            }
            return super.connectSocket(connectTimeout, socket, host, remoteAddress, localAddress, context);
        }
    }

    static class NoopHostnameVerifier implements javax.net.ssl.HostnameVerifier {
        public static final NoopHostnameVerifier INSTANCE = new NoopHostnameVerifier();

        @Override
        public boolean verify(String s, SSLSession sslSession) {
            return true;
        }
    }
}
