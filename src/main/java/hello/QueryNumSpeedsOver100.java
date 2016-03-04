package hello;
import org.apache.hadoop.conf.Configuration;

//import org.apache.hadoop.hbase.Cell;
//import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.HTableInterface;
//import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

//import java.io.ByteArrayInputStream;
//import java.io.DataInputStream;
//import java.security.MessageDigest;
//import java.security.NoSuchAlgorithmException;
//import java.util.Calendar;
//import java.util.Random;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
//import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
//import java.util.Map;
//import java.util.stream.Collectors;

/**
 * Created by Sean on 2/21/2016.
 */
public class QueryNumSpeedsOver100 {
    public String getResult(){
        //return "Query 1 result";
        String theResult = "";

        try
        {

            Configuration conf = HBaseConfiguration.create();
            HTable table = new HTable(conf, "highways");
            Scan scan = new Scan();
            //scan.setCaching(20);
            //scan.addFamily(Bytes.toBytes("highways"));

            //resultScanner scanner = table.getScanner(scan);

            Get theGet = new Get(Bytes.toBytes("1"));
            Result result = table.get(theGet);
            //get value first column
            //String inValue1 = Bytes.toString(result.value());
            //get value by ColumnFamily and ColumnName
            //byte[] inValueByte = result.getValue(Bytes.toBytes("highways"), Bytes.toBytes("highwayid"));
           // String inValue2 = Bytes.toString(inValueByte);

            for (Cell cell : result.listCells()) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                theResult += qualifier + ": " + value + "| ";
                //System.out.printf("Qualifier : %s : Value : %s", qualifier, value);
            }

            /*
            //create Map by result and print it
            //Map<String, String> getResult =  result.listCells().stream().collect(Collectors.toMap(e -> Bytes.toString(CellUtil.cloneQualifier(e)), e -> Bytes.toString(CellUtil.cloneValue(e))));
            //getResult.entrySet().stream().forEach(e-> System.out.printf("Qualifier : %s : Value : %s", e.getKey(), e.getValue()));
            Configuration config = HBaseConfiguration.create();
            Job job = Job.getInstance(config, "PageViewCounts");
            job.setJarByClass(QueryNumSpeedsOver100.class);     // class that contains mapper and reducer

            Scan scan = new Scan();
            scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
            scan.setCacheBlocks(false);  // don't set to true for MR jobs
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
            job.setNumReduceTasks(1);// at least one, adjust as required

            TableMapReduceUtil.addDependencyJars(job);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.hbase.util.Bytes.class);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.hbase.mapreduce.TableReducer.class);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.hbase.io.ImmutableBytesWritable.class);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil.class);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.hbase.mapreduce.TableMapper.class);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.io.IntWritable.class);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.io.LongWritable.class);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.mapreduce.Job.class);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.mapreduce.Mapper.class);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.mapreduce.Reducer.class);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.hbase.client.Put.class);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.hbase.client.Result.class);
            TableMapReduceUtil.addDependencyJars(job.getConfiguration(), org.apache.hadoop.hbase.client.Scan.class);

            //job.submit();
            boolean b = job.waitForCompletion(true);
            if (!b) {
                throw new IOException("error with job!");
            }
            // Setup Hadoop

            //Configuration conf = HBaseConfiguration.create();
            /*Job job = Job.getInstance(conf, "PageViewCounts");
            job.setJarByClass( QueryNumSpeedsOver100.class );

            // Create a scan
            Scan scan = new Scan();

            // Configure the Map process to use HBase
            TableMapReduceUtil.initTableMapperJob(

                    "freeway_loopdata",                    // The name of the table
                    scan,                           // The scan to execute against the table
                    MyMapper.class,                 // The Mapper class
                    LongWritable.class,             // The Mapper output key class
                    LongWritable.class,             // The Mapper output value class
                    job );                          // The Hadoop job

            // Configure the reducer process
            job.setReducerClass( MyReducer.class );
            job.setCombinerClass( MyReducer.class );

            // Setup the output - we'll write to the file system: HOUR_OF_DAY   PAGE_VIEW_COUNT
            job.setOutputKeyClass( LongWritable.class );
            job.setOutputValueClass( LongWritable.class );
            job.setOutputFormatClass( TextOutputFormat.class );

            // We'll run just one reduce task, but we could run multiple
            job.setNumReduceTasks( 1 );

            // Write the results to a file in the output directory
            FileOutputFormat.setOutputPath( job, new Path( "output" ) );
            TableMapReduceUtil.addDependencyJars(job);
            // Execute the job
            System.exit( job.waitForCompletion( true ) ? 0 : 1 );
        */
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }

        return theResult;
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

    static class MyMapper extends TableMapper<LongWritable, LongWritable> {
        private LongWritable ONE = new LongWritable(1);

        public MyMapper() {
        }

        @Override
        protected void map(ImmutableBytesWritable rowkey, Result columns, Mapper.Context context) throws IOException, InterruptedException

        {

            Integer speed = Integer.getInteger(Bytes.toString(columns.getValue("freeway_loopdata".getBytes(), "speed".getBytes())));
            if (speed > 100){
                context.write(new LongWritable(100), ONE);
            }

        }

    }

    static class MyReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>

    {
        public MyReducer() {
        }

        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException

        {
            // Add up all of the page views for this hour
            long sum = 0;
            for( LongWritable count : values )
            {
                sum += count.get();
            }

            // Write out the current hour and the sum
            context.write( key, new LongWritable( sum ) );
        }
    }


}


