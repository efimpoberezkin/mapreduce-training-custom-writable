package com.epam.training.bigdata.mapred.second;

import com.epam.training.bigdata.mapred.second.writable.FloatIntPairWritable;
import com.epam.training.bigdata.mapred.second.writable.IntPairWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reduces bytes sum and amount of requests to sum and average per request for IP.
 */
public class BytesByIPReducer extends Reducer<Text, IntPairWritable, Text, FloatIntPairWritable> {

    private FloatIntPairWritable resultAvgAndSum = new FloatIntPairWritable();

    @Override
    public void reduce(Text key, Iterable<IntPairWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        float count = 0;
        for (IntPairWritable value : values) {
            sum += value.getFirst();
            count += value.getSecond();
        }
        float average = sum / count;
        resultAvgAndSum.set(average, sum);
        context.write(key, resultAvgAndSum);
    }

}
