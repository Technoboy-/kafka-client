package com.owl.kafka.client.util;

/**
 * @Author: jiwei.guo
 * @Date: 2019/1/5 6:30 PM
 */
public class Pair<L, R> {

    private final L l;

    private final R r;

    public Pair(L l, R r){
        this.l = l;
        this.r = r;
    }

    public L getL() {
        return l;
    }

    public R getR() {
        return r;
    }
}
