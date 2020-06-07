package com.christy.innerJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Table implements Writable {
    /*
     * channel.txt:
     * 1001 01 1
     *
     * program.txt:
     * 01 地方新闻
     * 02 电影
     * 03 体育
     */

    private String channelId;
    private String programId;
    private int amount;
    private String programName;
    private String flag;

    public Table() {
        super();
    }

    public Table(String channelId, String programId, int amount, String programName, String flag) {
        super();
        this.channelId = channelId;
        this.programId = programId;
        this.amount = amount;
        this.programName = programName;
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(channelId);
        dataOutput.writeUTF(programId);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(programName);
        dataOutput.writeUTF(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.channelId = dataInput.readUTF();
        this.programId = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.programName = dataInput.readUTF();
        this.flag = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return this.channelId + " " + this.programName + " " + this.amount + " ";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj instanceof Table) {
            if (((Table) obj).amount == this.amount &&
                    ((Table) obj).programId.equals(this.programId) &&
                    ((Table) obj).channelId.equals(this.channelId) &&
                    ((Table) obj).programName.equals(this.programName) &&
                    ((Table) obj).flag.equals(this.flag)){
                return true;
            }
        }
        return false;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public String getProgramId() {
        return programId;
    }

    public void setProgramId(String programId) {
        this.programId = programId;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getProgramName() {
        return programName;
    }

    public void setProgramName(String programName) {
        this.programName = programName;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }
}
