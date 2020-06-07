package com.christy.topn;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RateBean implements Writable, Comparable<RateBean> {
    // 序号、频道名称、收视率、电视台所在地
    private String id;
    private String channelName;
    private Integer rate = 0;
    private String province;

    public RateBean() {
        super();
    }

    public RateBean(String id, String channelName, Integer rate, String province) {
        super();
        this.id = id;
        this.channelName = channelName;
        this.rate = rate;
        this.province = province;
    }

    public RateBean(RateBean bean) {
        this.id = bean.id;
        this.channelName = bean.channelName;
        this.rate = bean.rate;
        this.province = bean.province;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(channelName);
        dataOutput.writeInt(rate);
        dataOutput.writeUTF(province);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.channelName = dataInput.readUTF();
        this.rate = dataInput.readInt();
        this.province = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return id + " " + channelName + " " + rate + " " + province;
    }

    @Override
    public int compareTo(RateBean o) {
        return o.getRate() - this.getRate() == 0 ? this.rate.compareTo(o.getRate()) : o.getRate() - this.getRate();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj instanceof RateBean) {
            if (((RateBean) obj).channelName.equals(this.channelName) &&
                    ((RateBean) obj).id.equals(this.id) &&
                    ((RateBean) obj).rate.equals(this.rate) &&
                    ((RateBean) obj).province.equals(this.province)) {
                return true;
            }
        }
        return false;
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public Integer getRate() {
        return rate;
    }

    public void setRate(Integer rate) {
        this.rate = rate;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }
}
