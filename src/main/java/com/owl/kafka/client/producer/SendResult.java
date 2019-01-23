package com.owl.kafka.client.producer;

import java.io.Serializable;
import java.util.Objects;

/**
 * @Author: Tboy
 */
public class SendResult implements Serializable {

    public static SendResult ERROR_RESULT = new SendResult(-1, -1L);

    private int partition;

    private long offset;

    public SendResult(){
        //NOP
    }

    public SendResult(int partition, long offset){
        this.partition = partition;
        this.offset = offset;
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "partition : " + partition + "@offset : " + offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SendResult that = (SendResult) o;
        return partition == that.partition &&
                offset == that.offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(partition, offset);
    }
}
