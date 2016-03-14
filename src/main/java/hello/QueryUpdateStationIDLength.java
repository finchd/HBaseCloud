package hello;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by Sean on 2/21/2016.
 */
public class QueryUpdateStationIDLength {
    public String getResult(){
        try {
            Configuration conf = HBaseConfiguration.create();
            HTable table = new HTable(conf, "freeway_stations");

            // get the old result
            Get oldget = new Get(Bytes.toBytes("1140"));
            Result oldtable = table.get(oldget);
            String oldresult = Bytes.toString(oldtable.getValue(Bytes.toBytes("freeway_stations"),
                    Bytes.toBytes("length")));

            // Set the new result
            Put p = new Put(Bytes.toBytes("1140"));
            p.addColumn(Bytes.toBytes("freeway_stations"), Bytes.toBytes("length"),Bytes.toBytes("2.3"));
            table.put(p); // This command changes the value

            // get the new result
            Get newget = new Get(Bytes.toBytes("1140"));
            Result newtable = table.get(newget);
            String newresult = Bytes.toString(newtable.getValue(Bytes.toBytes("freeway_stations"),
                    Bytes.toBytes("length")));

            // Put the old value back to how it was so we can do this again
            Put backp = new Put(Bytes.toBytes("1140"));
            backp.addColumn(Bytes.toBytes("freeway_stations"), Bytes.toBytes("length"),Bytes.toBytes("2.14"));
            table.put(backp); // This command changes the value


            return "Query 5: update the value of stationID: 1140 to have a length of 2.3 <br>" +
                    "StationID has an OLD length value of: " + oldresult + "<br>" +
                    "StationID has a NEW length value value of " + newresult;

        } catch (Exception e) {
            e.printStackTrace();
            return "the query was a failure";
        }
    }
}
