package org.apache.rocketmq.connect.http.sink.auth;

public class SocksProxyConfig {

    private String socks5Endpoint;
    private String socks5UserName;
    private String socks5Password;

    public SocksProxyConfig(String socks5Endpoint, String socks5UserName, String socks5Password) {
        this.socks5Endpoint = socks5Endpoint;
        this.socks5UserName = socks5UserName;
        this.socks5Password = socks5Password;
    }

    public String getSocks5Endpoint() {
        return socks5Endpoint;
    }

    public void setSocks5Endpoint(String socks5Endpoint) {
        this.socks5Endpoint = socks5Endpoint;
    }

    public String getSocks5UserName() {
        return socks5UserName;
    }

    public void setSocks5UserName(String socks5UserName) {
        this.socks5UserName = socks5UserName;
    }

    public String getSocks5Password() {
        return socks5Password;
    }

    public void setSocks5Password(String socks5Password) {
        this.socks5Password = socks5Password;
    }
}
