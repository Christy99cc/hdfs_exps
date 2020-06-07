package com.christy.tvTime;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TvTime implements Writable {
    private Integer day;
    private Integer night;
    public TvTime(){
        super();
    }
    @Override // 序列化
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(day);
        dataOutput.writeInt(night);
    }
    @Override    // 反序列化，与序列化顺序一致
    public void readFields(DataInput dataInput) throws IOException {
        this.day = dataInput.readInt();
        this.night = dataInput.readInt();
    }
    @Override
    public String toString() {
        return day + "\t" + night;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj instanceof TvTime){
            if (((TvTime) obj).day.equals(this.day)&&((TvTime) obj).night.equals(this.night)){
                return true;
            }
        }
        return false;
    }
    /*
     * setters and getters
     */

    public Integer getDay() {
        return day;
    }

    public void setDay(Integer day) {
        this.day = day;
    }

    public Integer getNight() {
        return night;
    }

    public void setNight(Integer night) {
        this.night = night;
    }

}
