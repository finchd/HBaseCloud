package hello;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * Created by Sean on 2/21/2016.
 */
public class QueryPeakTravelTimeFosterNB {
    public String getResult(){
        try {
            Configuration conf = HBaseConfiguration.create();
            HTable table = new HTable(conf, "results");

            Get morningGet = new Get(Bytes.toBytes("Morning Peak Speed:"));
            Result morningResult = table.get(morningGet);
            int morningPeakSpeed = Integer.parseInt(Bytes.toString(morningResult.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            Get totMornGet = new Get(Bytes.toBytes("Total Morning Records"));
            Result totMornResult= table.get(totMornGet);
            int totalMorningRecords = Integer.parseInt(Bytes.toString(totMornResult.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            Get rushPeakGet = new Get(Bytes.toBytes("Rush Peak Speed:"));
            Result rushPeakResult = table.get(rushPeakGet);
            int rushPeakSpeeds = Integer.parseInt(Bytes.toString(rushPeakResult.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            Get totRushGet = new Get(Bytes.toBytes("Total Rush Records"));
            Result totRushResult = table.get(totRushGet);
            int totalRushRecords = Integer.parseInt(Bytes.toString(totRushResult.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            double avgSpeedMorning = morningPeakSpeed/(double)totalMorningRecords;
            double avgSpeedRush = rushPeakSpeeds/(double)totalRushRecords;

            double morningTravelSecondsPerMile = (60.0/avgSpeedMorning)*60.0;
            double rushTravelSecondsPerMile = (60.0/avgSpeedRush)*60.0;

            double travelTimeFosterMorning = 1.6*morningTravelSecondsPerMile;
            double travelTimeFosterRush = 1.6*rushTravelSecondsPerMile;

            String rowStart = "<tr>";
            String rowEnd = "</td>";
            String lineBreak = "<br>";

            return "<h1>Peak Travel time on I-205 Foster NB</h1>" + lineBreak + "<table border=\"1\">" +
                    rowStart + "<td>" + "Time" + "</td><td>" + "Travel time in Seconds" + "</td>" + rowEnd +
                    rowStart + "<td>7-9am</td><td>" + (int)travelTimeFosterMorning + "</td>" + rowEnd +
                    rowStart + "<td>4-6pm</td><td>" + (int)travelTimeFosterRush + "</td>" + rowEnd;

        } catch (Exception e) {
            e.printStackTrace();
            return "the query was a failure";
        }
    }
}
