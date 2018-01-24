package com.epam.training.bigdata.mapred.second;

import com.epam.training.bigdata.mapred.second.writable.FloatIntPairWritable;
import com.epam.training.bigdata.mapred.second.writable.IntPairWritable;
import com.github.sakserv.minicluster.util.WindowsLibsUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Driver class for a Bytes by IP MapReduce job.
 * Main method should be run with input and output paths as well as output format as parameters.
 *
 * Output format can be:
 * csv - for csv file,
 * seq - for compressed sequence file.
 */
public class BytesByIP {

    /**
     * Can be run with "src\main\resources\input src\main\resources\output csv" as program arguments.
     */
    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_HOME", "C:/hadoop-mini-clusters");
        WindowsLibsUtils.setHadoopHome();
        MRLocalClusterApp.main(null);

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        if ("seq".equals(args[2])) {
            conf.set("mapreduce.output.fileoutputformat.compress", "true");
            conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
            conf.set("mapreduce.output.fileoutputformat.compress.type", "BLOCK");
        }

        Job job = Job.getInstance(conf, "Bytes by IP");
        job.setJarByClass(BytesByIP.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        if ("csv".equals(args[2])) {
            TextOutputFormat.setOutputPath(job, new Path(args[1]));
        }
        else if ("seq".equals(args[2])) {
            SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
        }

        job.setMapperClass(BytesByIPMapper.class);
        job.setCombinerClass(SumAndCountCombiner.class);
        job.setReducerClass(BytesByIPReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatIntPairWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntPairWritable.class);

        int jobResult = job.waitForCompletion(true) ? 0 : 1;

        job.getCounters().getGroup("Browsers").forEach(
                counter -> System.out.println(counter.getDisplayName() + ": " + counter.getValue())
        );

        System.exit(jobResult);
    }
}
