package hello;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by Sean on 2/21/2016.
 */
public class QueryTotalVolumeFosterNB {
    public String getResult(){
        String theResult = "";
        try {
            Configuration conf = HBaseConfiguration.create();
            HTable table = new HTable(conf, "results");

            Get getVolume = new Get(Bytes.toBytes("Total Volume:"));
            Result resultVolume = table.get(getVolume);
            String volume = Bytes.toString(resultVolume.getValue(Bytes.toBytes("results"), Bytes.toBytes("count")));

            theResult += "<h1>Total Volume for Foster NB on Sept 21, 2011</h1>";
            theResult += "<br>";
            theResult +="<p>Total Volume: " + volume + "</p>";




        } catch (Exception e) {
            e.printStackTrace();
        }
        return theResult;
    }
}
