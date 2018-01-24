package com.epam.training.bigdata.mapred.second;

import com.epam.training.bigdata.mapred.second.writable.IntPairWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduces bytes sum and amount of requests for IP.
 */
public class SumAndCountCombiner extends Reducer<Text, IntPairWritable, Text, IntPairWritable> {

    private IntPairWritable resultSumAndCount = new IntPairWritable();

    @Override
    public void reduce(Text key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        for (IntPairWritable value : values) {
            sum += value.getFirst();
            count += value.getSecond();
        }
        resultSumAndCount.set(sum, count);
        context.write(key, resultSumAndCount);
    }
}
