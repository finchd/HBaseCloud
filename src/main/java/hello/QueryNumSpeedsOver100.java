package hello;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

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
            theResult = "Speeds over 100: " + Bytes.toString(result.getValue(Bytes.toBytes("results"), Bytes.toBytes("count")));
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }

        return theResult;
    }
}


