package hello;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.Random;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
/**
 * Created by Sean on 2/21/2016.
 */
public class QueryNumSpeedsOver100 {
    public String getResult(){
        //return "Query 1 result";

        try
        {
            Configuration config = HBaseConfiguration.create();
            Job job = Job.getInstance(config, "PageViewCounts");
            job.setJarByClass(QueryNumSpeedsOver100.class);     // class that contains mapper and reducer

            Scan scan = new Scan();
            //scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
            //scan.setCacheBlocks(false);  // don't set to true for MR jobs
            // set other scan attrs

            TableMapReduceUtil.initTableMapperJob(
                    "freeway_loopdata",        // input table
                    scan,               // Scan instance to control CF and attribute selection
                    MyMapper1.class,     // mapper class
                    Text.class,         // mapper output key
                    IntWritable.class,  // mapper output value
                    job);
            TableMapReduceUtil.initTableReducerJob(
                    "results",        // output table
                    MyTableReducer.class,    // reducer class
                    job);
            job.setNumReduceTasks(1);   // at least one, adjust as required

            job.submit();
            //boolean b = job.waitForCompletion(true);
           // if (!b) {
            //    throw new IOException("error with job!");
            //}
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }

        return "stored in results table";
    }

    public static class MyMapper1 extends TableMapper<Text, IntWritable>  {
        public static final byte[] CF = "freeway_loopdata".getBytes();
        public static final byte[] ATTR1 = "speed".getBytes();

        private final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            String val = new String(value.getValue(CF, ATTR1));
            Integer speed = Integer.getInteger(val);

            if (speed > 100){
                text.set("Speeds Over 100:");
                context.write(text, ONE);
            }
        }
    }

    public static class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        public static final byte[] CF = "Results".getBytes();
        public static final byte[] COUNT = "count".getBytes();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (IntWritable val : values) {
                i += val.get();
            }
            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(CF, COUNT, Bytes.toBytes(i));

            context.write(null, put);
        }
    }

}


