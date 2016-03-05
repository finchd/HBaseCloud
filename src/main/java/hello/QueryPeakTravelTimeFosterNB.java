package hello;

/**
 * Created by Sean on 2/21/2016.
 */
public class QueryPeakTravelTimeFosterNB {
    public String getResult(){

        // Map: takes a locationID (Foster NB) and (locationID, detectorID, timestamp, speed) tuples and outputs only
        // tuples that match the location (Foster NB) and time.
        // Reduce: sums speed, then average by dividing by station length


        //Configuration conf = HBaseConfiguration.create();
        //HTable table = new HTable(conf, "highways"); // TODO: change table to results from MapReduce job
        //Scan scan = new Scan();

        // Here, just query output table from MapReduce, and post-process (ie sorting)

        return "Query 3 result";
    }
}
