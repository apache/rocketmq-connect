package org.apache.rocketmq.connect.http.sink.entity;

public class OAuthParameters {

    private String authorizationEndpoint;
    private ClientParameters clientParameters;
    private String httpMethod;
    private OAuthHttpParameters oauthHttpParameters;

    public String getAuthorizationEndpoint() {
        return authorizationEndpoint;
    }

    public void setAuthorizationEndpoint(String authorizationEndpoint) {
        this.authorizationEndpoint = authorizationEndpoint;
    }

    public ClientParameters getClientParameters() {
        return clientParameters;
    }

    public void setClientParameters(ClientParameters clientParameters) {
        this.clientParameters = clientParameters;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public void setHttpMethod(String httpMethod) {
        this.httpMethod = httpMethod;
    }

    public OAuthHttpParameters getOauthHttpParameters() {
        return oauthHttpParameters;
    }

    public void setOauthHttpParameters(OAuthHttpParameters oauthHttpParameters) {
        this.oauthHttpParameters = oauthHttpParameters;
    }

    public static class ClientParameters {
        private String clientID;

        private String clientSecret;

        public String getClientID() {
            return clientID;
        }

        public void setClientID(String clientID) {
            this.clientID = clientID;
        }

        public String getClientSecret() {
            return clientSecret;
        }

        public void setClientSecret(String clientSecret) {
            this.clientSecret = clientSecret;
        }
    }

}
