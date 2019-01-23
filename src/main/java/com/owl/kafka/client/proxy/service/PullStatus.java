package com.owl.kafka.client.proxy.service;

/**
 * @Author: Tboy
 */
public enum PullStatus {

    FOUND((byte)1),

    NO_NEW_MSG((byte)0);

    private byte status;

    private PullStatus(byte status){
        this.status = status;
    }

    public byte getStatus() {
        return status;
    }

    public static PullStatus of(byte status){
        for(PullStatus ps : values()){
            if(ps.getStatus() == status){
                return ps;
            }
        }
        return null;
    }
}
