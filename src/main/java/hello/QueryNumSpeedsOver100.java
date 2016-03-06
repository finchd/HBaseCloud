package hello;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Created by Sean on 2/21/2016.
 */
public class QueryNumSpeedsOver100 {
    public String getResult() {
        String theResult = "";
        try {
            Configuration conf = HBaseConfiguration.create();
            HTable table = new HTable(conf, "results");

            Get theGet70 = new Get(Bytes.toBytes("Speeds over 70:"));
            Result result70 = table.get(theGet70);
            int over70 = Integer.parseInt(Bytes.toString(result70.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            Get theGet80 = new Get(Bytes.toBytes("Speeds over 80:"));
            Result result80 = table.get(theGet80);
            int over80 = Integer.parseInt(Bytes.toString(result80.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            Get theGet90 = new Get(Bytes.toBytes("Speeds over 90:"));
            Result result90 = table.get(theGet90);
            int over90 = Integer.parseInt(Bytes.toString(result90.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            Get theGet100 = new Get(Bytes.toBytes("Speeds over 100:"));
            Result result100 = table.get(theGet100);
            int over100 = Integer.parseInt(Bytes.toString(result100.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            Get theGet102 = new Get(Bytes.toBytes("Speeds over 102:"));
            Result result102 = table.get(theGet102);
            int over102 = Integer.parseInt(Bytes.toString(result102.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            Get theGet104 = new Get(Bytes.toBytes("Speeds over 104:"));
            Result result104 = table.get(theGet104);
            int over104 = Integer.parseInt(Bytes.toString(result104.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            Get theGet106 = new Get(Bytes.toBytes("Speeds over 106:"));
            Result result106 = table.get(theGet106);
            int over106 = Integer.parseInt(Bytes.toString(result106.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            Get theGetTotalSpeed = new Get(Bytes.toBytes("Total Speed:"));
            Result resultTotalSpeed = table.get(theGetTotalSpeed);
            int totalSpeed = Integer.parseInt(Bytes.toString(resultTotalSpeed.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            Get theGetTotalRecords = new Get(Bytes.toBytes("Total speed records"));
            Result resultTotalRecords = table.get(theGetTotalRecords);
            int totalSpeedRecords = Integer.parseInt(Bytes.toString(resultTotalRecords.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));

            double avgSpeed = totalSpeed / totalSpeedRecords;
            double percentOver70 = over70 / (double) totalSpeedRecords * 100;
            double percentOver80 = over80 / (double) totalSpeedRecords * 100;
            double percentOver90 = over90 / (double) totalSpeedRecords * 100;
            double percentOver100 = over100 / (double) totalSpeedRecords * 100;
            double percentOver102 = over102 / (double) totalSpeedRecords * 100;
            double percentOver104 = over104 / (double) totalSpeedRecords * 100;
            double percentOver106 = over106 / (double) totalSpeedRecords * 100;

            String lineBreak = "<br>";
            theResult += "<h1>Speeders on I205</h1>";
            theResult += lineBreak;
            theResult += "Speeds over 70: " + over70 + "\tPercentage of total: " + round(percentOver70, 4) + "%";
            theResult += lineBreak;
            theResult += "Speeds over 80: " + over80 + "\tPercentage of total: " + round(percentOver80, 4) + "%";
            theResult += lineBreak;
            theResult += "Speeds over 90: " + over90 + "\tPercentage of total: " + round(percentOver90, 4) + "%";
            theResult += lineBreak;
            theResult += "Speeds over 100: " + over100 + "\tPercentage of total: " + round(percentOver100, 4) + "%";
            theResult += lineBreak;
            theResult += "Speeds over 102: " + over102 + "\tPercentage of total: " + round(percentOver102, 4) + "%";
            theResult += lineBreak;
            theResult += "Speeds over 104: " + over104 + "\tPercentage of total: " + round(percentOver104, 4) + "%";
            theResult += lineBreak;
            theResult += "Speeds over 106: " + over106 + "\tPercentage of total: " + round(percentOver106, 4) + "%";
            theResult += lineBreak;
            theResult += lineBreak;
            theResult += "Avg speed: " + avgSpeed + " Total speed records " + totalSpeedRecords;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return theResult;
    }


    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();

        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }
}
