package com.github.liyue2008.ipcount;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author LiYue
 * Date: 2019-09-15
 */
public class IpAndCount {
    private Date date;
    private String ip;
    private long count = 0L;

    public IpAndCount(){}
    public IpAndCount(Date date, String ip, long count) {
        this.date = date;
        this.ip = ip;
        this.count = count;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").format(getDate()) + " " + getIp() + " " + getCount();
    }
}
