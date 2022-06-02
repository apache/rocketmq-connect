package org.apache.rocketmq.connect.http.sink.entity;


public class NetworkParameters {

    private String networkType;

    private String vpcId;

    private String vswitcheId;

    private String securityGroupId;

    public String getNetworkType() {
        return networkType;
    }

    public void setNetworkType(String networkType) {
        this.networkType = networkType;
    }

    public String getVpcId() {
        return vpcId;
    }

    public void setVpcId(String vpcId) {
        this.vpcId = vpcId;
    }

    public String getVswitcheId() {
        return vswitcheId;
    }

    public void setVswitcheId(String vswitcheId) {
        this.vswitcheId = vswitcheId;
    }

    public String getSecurityGroupId() {
        return securityGroupId;
    }

    public void setSecurityGroupId(String securityGroupId) {
        this.securityGroupId = securityGroupId;
    }
}
