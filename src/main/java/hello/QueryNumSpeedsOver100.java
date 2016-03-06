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
        String theResult = "";
        try
        {
            Configuration conf = HBaseConfiguration.create();
            HTable table = new HTable(conf, "results");
            Get theGet = new Get(Bytes.toBytes("Speeds over 100:"));
            Result result = table.get(theGet);
            theResult = "Speeds over 100: " + Bytes.toString(result.getRow());//result.getValue(Bytes.toBytes("results"), Bytes.toBytes("count")));
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }

        return theResult;
    }
}


