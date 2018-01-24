package com.epam.training.bigdata.mapred.second.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


/**
 * Custom Writable class for pair of integers.
 */
public class IntPairWritable implements Writable {

    private int first;
    private int second;

    public IntPairWritable() {
    }

    public IntPairWritable(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public void set(int first, int second) {
        this.first = first;
        this.second = second;
    }

    public int getFirst() {
        return first;
    }

    public int getSecond() {
        return second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = in.readInt();
        second = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntPairWritable that = (IntPairWritable) o;
        return first == that.first &&
                second == that.second;
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }

    @Override
    public String toString() {
        return "IntPairWritable{" + first + ", " + second + '}';
    }
}
