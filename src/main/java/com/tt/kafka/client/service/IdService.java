package com.tt.kafka.client.service;

import com.tt.kafka.util.Preconditions;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Calendar;

/**
 * @Author: Tboy
 */
public class IdService {

    /**
     * 时间偏移量，从2016年11月1日零点开始
     */
    public static final long EPOCH;

    /**
     * 自增量占用比特
     */
    private static final long SEQUENCE_BITS = 12L;
    /**
     * 工作进程ID比特
     */
    private static final long WORKER_ID_BITS = 10L;
    /**
     * 自增量掩码（最大值）
     */
    private static final long SEQUENCE_MASK = (1 << SEQUENCE_BITS) - 1;
    /**
     * 工作进程ID左移比特数（位数）
     */
    private static final long WORKER_ID_LEFT_SHIFT_BITS = SEQUENCE_BITS;
    /**
     * 时间戳左移比特数（位数）
     */
    private static final long TIMESTAMP_LEFT_SHIFT_BITS = WORKER_ID_LEFT_SHIFT_BITS + WORKER_ID_BITS;
    /**
     * 工作进程ID最大值
     */
    private static final long WORKER_ID_MAX_VALUE = 1L << WORKER_ID_BITS;

    /**
     * 工作进程ID
     */
    private static long workerId;

    static {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2016, Calendar.NOVEMBER, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        EPOCH = calendar.getTimeInMillis();
    }

    static {
        initWorkerId();
    }

    static void initWorkerId() {
        InetAddress address;
        try {
            address = InetAddress.getLocalHost();
        } catch (final UnknownHostException e) {
            throw new IllegalStateException("Cannot get LocalHost InetAddress, please check your network!");
        }
        byte[] ipAddressByteArray = address.getAddress();
        workerId = ((long) (((ipAddressByteArray[ipAddressByteArray.length - 2] & 0B11) << Byte.SIZE) + (ipAddressByteArray[ipAddressByteArray.length - 1] & 0xFF)));
    }

    public static IdService I = new IdService();

    private IdService(){

    }

    /**
     * 最后自增量
     */
    private long sequence;
    /**
     * 最后生成编号时间戳，单位：毫秒
     */
    private long lastTime;

    /**
     * 生成Id.
     *
     * @return 返回@{@link Long}类型的Id
     */
    public synchronized long getId() {
        // 保证当前时间大于最后时间。时间回退会导致产生重复id
        long currentMillis = System.currentTimeMillis();
        Preconditions.checkArgument(lastTime <= currentMillis, "Clock is moving backwards, last time is " + lastTime + " milliseconds, current time is " + currentMillis +  " milliseconds");
        // 获取序列号
        if (lastTime == currentMillis) {
            if (0L == (sequence = ++sequence & SEQUENCE_MASK)) { // 当获得序号超过最大值时，归0，并去获得新的时间
                currentMillis = waitUntilNextTime(currentMillis);
            }
        } else {
            sequence = 0;
        }
        // 设置最后时间戳
        lastTime = currentMillis;
        // 生成编号
        return ((currentMillis - EPOCH) << TIMESTAMP_LEFT_SHIFT_BITS) | (workerId << WORKER_ID_LEFT_SHIFT_BITS) | sequence;
    }

    /**
     * 不停获得时间，直到大于最后时间
     *
     * @param lastTime 最后时间
     * @return 时间
     */
    private long waitUntilNextTime(final long lastTime) {
        long time = System.currentTimeMillis();
        while (time <= lastTime) {
            time = System.currentTimeMillis();
        }
        return time;
    }
}
