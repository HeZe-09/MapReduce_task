package com.hadoop.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Author Daniel
 * @Description
 **/
public class SortBean implements WritableComparable<SortBean> {
    // 根据需求来定义
    private String name;
    private int value;

    public SortBean() {
    }


    public SortBean(String name, int value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return name+"\t"+value;
    }

    @Override
    public int compareTo(SortBean o) {
        return o.value - this.value;


    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(value);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        name = in.readUTF();
        value = in.readInt();
    }
}
