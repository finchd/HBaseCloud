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
            System.out.println("Step1");
            Get theGet70 = new Get(Bytes.toBytes("Speeds over 70:"));
            Result result70 = table.get(theGet70);
            int over70 = Integer.parseInt(Bytes.toString(result70.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));
            System.out.println("Step2");
            Get theGet80 = new Get(Bytes.toBytes("Speeds over 80:"));
            Result result80 = table.get(theGet80);
            int over80 = Integer.parseInt(Bytes.toString(result80.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));
            System.out.println("Step3");
            Get theGet90 = new Get(Bytes.toBytes("Speeds over 90:"));
            Result result90 = table.get(theGet90);
            int over90 = Integer.parseInt(Bytes.toString(result90.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));
            System.out.println("Step4");
            Get theGet100 = new Get(Bytes.toBytes("Speeds over 100:"));
            Result result100 = table.get(theGet100);
            int over100 = Integer.parseInt(Bytes.toString(result100.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));
            System.out.println("Step5");
            Get theGet102 = new Get(Bytes.toBytes("Speeds over 102:"));
            Result result102 = table.get(theGet102);
            int over102 = Integer.parseInt(Bytes.toString(result102.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));
            System.out.println("Step6");
            Get theGet104 = new Get(Bytes.toBytes("Speeds over 104:"));
            Result result104 = table.get(theGet104);
            System.out.println("Step7");
            int over104 = Integer.parseInt(Bytes.toString(result104.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));
            Get theGet106 = new Get(Bytes.toBytes("Speeds over 106:"));
            Result result106 = table.get(theGet106);
            System.out.println("Step8");
            int over106 = Integer.parseInt(Bytes.toString(result106.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));
            System.out.println("Step9");
            Get theGet108 = new Get(Bytes.toBytes("Speeds over 108:"));
            Result result108 = table.get(theGet108);
            int over108 = Integer.parseInt(Bytes.toString(result108.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));
            System.out.println("Step10");
            Get theGetTotalSpeed = new Get(Bytes.toBytes("Total Speed:"));
            Result resultTotalSpeed = table.get(theGetTotalSpeed);
            int totalSpeed = Integer.parseInt(Bytes.toString(resultTotalSpeed.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));
            System.out.println("Step11");
            Get theGetTotalRecords = new Get(Bytes.toBytes("Total speed records"));
            Result resultTotalRecords = table.get(theGetTotalRecords);
            int totalSpeedRecords = Integer.parseInt(Bytes.toString(resultTotalRecords.getValue(Bytes.toBytes("results"), Bytes.toBytes("count"))));
            System.out.println("Step12");

            double avgSpeed = totalSpeed/totalSpeedRecords;
            double percentOver70 = over70/(double)totalSpeedRecords * 100;
            double percentOver80 = over80/(double)totalSpeedRecords * 100;
            double percentOver90 = over90/(double)totalSpeedRecords * 100;
            double percentOver100 = over100/(double)totalSpeedRecords * 100;
            double percentOver102 = over102/(double)totalSpeedRecords * 100;
            double percentOver104 = over104/(double)totalSpeedRecords * 100;
            double percentOver106 = over106/(double)totalSpeedRecords * 100;
            double percentOver108 = over108/(double)totalSpeedRecords * 100;
            String lineBreak = "<br>";

            theResult = "Speeds over 70: " + over70 + " percentage of total: " + percentOver70 + "%";
            theResult += lineBreak;
            theResult += "Speeds over 80: " + over80 + " percentage of total: " + percentOver80 + "%";
            theResult += lineBreak;
            theResult += "Speeds over 90: " + over90 + " percentage of total: " + percentOver90 + "%";
            theResult += lineBreak;
            theResult += "Speeds over 100: " + over100 + " percentage of total: " + percentOver100 + "%";
            theResult += lineBreak;
            theResult += "Speeds over 102: " + over102 + " percentage of total: " + percentOver102 + "%";
            theResult += lineBreak;
            theResult += "Speeds over 104: " + over104 + " percentage of total: " + percentOver104 + "%";
            theResult += lineBreak;
            theResult += "Speeds over 106: " + over106 + " percentage of total: " + percentOver106  + "%";
            theResult += lineBreak;
            theResult += "Speeds over 108: " + over108 + " percentage of total: " + percentOver108 + "%";
            theResult += lineBreak;
            theResult += "Avg speed: " + avgSpeed + " Total speed records " + totalSpeedRecords;

            //theResult = "Speeds over 100: " + Bytes.toString(result100.getValue(Bytes.toBytes("results"), Bytes.toBytes("count")));
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }

        return theResult;
    }
}


