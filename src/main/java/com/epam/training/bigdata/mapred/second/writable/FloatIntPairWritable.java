package com.epam.training.bigdata.mapred.second.writable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;


/**
 * Custom Writable class for pair of float and integer.
 */
public class FloatIntPairWritable implements Writable {

    private float floatValue;
    private int intValue;

    public FloatIntPairWritable() {
    }

    public FloatIntPairWritable(float floatValue, int intValue) {
        this.floatValue = floatValue;
        this.intValue = intValue;
    }

    public void set(float floatValue, int intValue) {
        this.floatValue = floatValue;
        this.intValue = intValue;
    }

    public float getFloatValue() {
        return floatValue;
    }

    public int getIntValue() {
        return intValue;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(floatValue);
        out.writeInt(intValue);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        floatValue = in.readFloat();
        intValue = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FloatIntPairWritable that = (FloatIntPairWritable) o;
        return Float.compare(that.floatValue, floatValue) == 0 &&
                intValue == that.intValue;
    }

    @Override
    public int hashCode() {

        return Objects.hash(floatValue, intValue);
    }

    @Override
    public String toString() {
        return floatValue + "," + intValue;
    }
}
