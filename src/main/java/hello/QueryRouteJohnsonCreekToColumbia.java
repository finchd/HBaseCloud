package hello;

        import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.hbase.HBaseConfiguration;
        import org.apache.hadoop.hbase.client.*;
        import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by Scott Fabini on 3/1/2016.
 */
public class QueryRouteJohnsonCreekToColumbia {
    private final static String startingLocationText = "Johnson Cr NB";
    private final static String endingLocationText = "Columbia to I-205 NB";
    private final static int lowestNorthBoundStationId = 1045;
    private HTable table;
    private Record<String, String, String> endingStation;
    private Record<String, String, String> nextStation;
    private Record<String, String, String> currentStation;
    /**
     * The main routine for this query.
     * Loop on the downstream stationids until we hit our stopping conditions:
     * 1) hit the stationid = 0; return "No route found"
     * 2) a result.isEmpty(); return "No route found"
     * 3) the corresponding endingStation stationid; return theResult
     * @return a string which a sequence of station ids and location texts corresponding
     * to the route from Johnson Cr NB to Columbia to I-205 NB", or "No route found"
     */
    public String getResult(){
        initHBaseQuery();
        initRouteFindQuery();
        String theResult = "";

        String rowStart = "<tr>";
        String rowEnd = "</td>";

        String lineBreak = "<br>";
        theResult += "<h1>Route Finding: Johnson Creek NB to Columbia Blvd NB</h1>";
        theResult += lineBreak;

        theResult += "<table border=\"1\">";
        theResult += rowStart;
        theResult += "<tr><td>Station ID</td>" + "<td>Location Text</td></tr>";
        theResult += rowEnd + rowStart;
        theResult += "<tr><td>" + currentStation.getKey() + "</td>";
        theResult += "<td>" + currentStation.getValue1() + "</td></tr>";

        try
        {
            Result tableResult;
            do {
                Get theGet = new Get(Bytes.toBytes(nextStation.getKey()));
                tableResult = table.get(theGet);

                if (tableResult.isEmpty()) {
                    theResult = "No route found.";
                    return theResult;
                }

                nextStation = getNextStationFromDownstream(Integer.parseInt(currentStation.getKey()));

                // matches endingStation.  return result
                if (nextStation.getKey().equals(endingStation.getKey())) {

                    theResult += "<tr><td>" + nextStation.getKey() + "</td>" +
                            "<td>" + nextStation.getValue1() + "</td></tr>";
                    return theResult;

                }
                // nextStation = 0; no route found.
                else if (Integer.parseInt(nextStation.getKey()) == 0) {
                    theResult = "No route found.";
                    return theResult;
                }
                // add intermediate station result to theResult string
                else {
                    theResult += "<tr><td>" + currentStation.getKey() + "</td>" +
                                 "<td>" + currentStation.getValue1() + "</td></tr>";
                    currentStation = nextStation;
                }

            } while (!tableResult.isEmpty() && Integer.parseInt(nextStation.getKey()) != 0);
        }
        catch( Exception e )
        {
            e.printStackTrace();
        }

        return theResult;
    }



    /**
     * Initialize the HBase query configuration
     */
    public void initHBaseQuery() {
        Configuration conf;
        try {

            // Use this to configure.  Currently works just with localhost
            conf = HBaseConfiguration.create();

            // conf.setStrings("hostname".....);
            table = new HTable(conf, "freeway_stations");
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * set the startingStation and endingStation variables, which will be used
     * throughout the main routine.  Initialize the first nextStation variable.
     */
    public void initRouteFindQuery() {
        // Setup the query:
        // Get the <stationid, locationtext> pairs corresponding to our starting and ending
        // locationtext, and the first downstream after the startingStation
        Record<String, String, String> startingStation = getStationFromLocationText(startingLocationText);
        currentStation = startingStation;
        endingStation = getStationFromLocationText(endingLocationText);
        nextStation = getNextStationFromDownstream(Integer.parseInt(startingStation.getValue2()));
    }

    /**
     * Generic Record class, to hold our stationid, locationtext, downstream records
     * @param <K> The stationid
     * @param <V1> The locationtext
     * @param <V2> The downstream
     */
    class Record<K, V1, V2> {
        private K k;
        private V1 v1;
        private V2 v2;

        public Record(K k, V1 v1, V2 v2) {
            this.k = k;
            this.v1 = v1;
            this.v2 = v2;
        }
        public K getKey() {
            return k;
        }
        public V1 getValue1() {
            return v1;
        }
        public V2 getValue2() {
            return v2;
        }
    }


    /**
     * Get the station record that corresponds to the matching locationtext
     * @param locationtext the locationtext to match
     * @return the corresponding (stationid, locationtext, downstream) record
     */
    public Record<String,String, String> getStationFromLocationText(String locationtext) {
        Result result;
        String locationtextRead;
        String downstreamRead;
        String stationidRead;
        Record<String, String, String> station;

        int stationId = lowestNorthBoundStationId;
        try {
            do {
                // Search sequentially, starting from the lowestNorthBoundStationId
                Get theGet = new Get(Bytes.toBytes(String.valueOf(stationId)));
                result = table.get(theGet);

                if (result != null) {
                    // Get the locationtext
                    byte[] locationtextByte = result.getValue(Bytes.toBytes("freeway_stations"), Bytes.toBytes("locationtext"));
                    locationtextRead = Bytes.toString(locationtextByte);
                    // Get the corresponding stationid
                    byte[] stationidByte = result.getValue(Bytes.toBytes("freeway_stations"), Bytes.toBytes("stationid"));
                    stationidRead = Bytes.toString(stationidByte);
                    // Get the corresponding downstream stationid
                    byte[] downstreamByte = result.getValue(Bytes.toBytes("freeway_stations"), Bytes.toBytes("downstream"));
                    downstreamRead = Bytes.toString(downstreamByte);
                    // Get the corresponding upstream stationid
                    // byte[] upstreamByte = result.getValue(Bytes.toBytes("freeway_stations"), Bytes.toBytes("downstream"));
                    // String upstreamRead = Bytes.toString(upstreamByte);

                    // If matching locationTextRead found, return the stationidRead
                    if (locationtext.matches(locationtextRead)) {
                        station = new Record<>(stationidRead, locationtextRead, downstreamRead);
                        return station;
                    }

                    // Get the next downstream stationId for next iteration
                    stationId = Integer.parseInt(downstreamRead);
                }
            } while (!result.isEmpty() && stationId != 0);
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }

        return new Record<>(null, null, null);
    }

    /**
     * Get the next downstream station record
     * @param downstream the downstream stationid
     * @return The (stationid, locationtext, downstream) record corresponding to the downstream
     * stationid
     */
    public Record<String,String, String> getNextStationFromDownstream(int downstream) {
        Result result = null;
        String locationtextRead = null;
        String downstreamRead = null;
        String stationidRead = null;

        try {
            Get theGet = new Get(Bytes.toBytes(String.valueOf(downstream)));
            result = table.get(theGet);
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }

        if (result != null) {
            // Get the next downstream's locationtext
            byte[] stationidByte = result.getValue(Bytes.toBytes("freeway_stations"), Bytes.toBytes("stationid"));
            stationidRead = Bytes.toString(stationidByte);
            // Get the next downstream's locationtext
            byte[] locationtextByte = result.getValue(Bytes.toBytes("freeway_stations"), Bytes.toBytes("locationtext"));
            locationtextRead = Bytes.toString(locationtextByte);
            // Get the next downstream
            byte[] downstreamByte = result.getValue(Bytes.toBytes("freeway_stations"), Bytes.toBytes("downstream"));
            downstreamRead = Bytes.toString(downstreamByte);
        }

        return new Record<>(stationidRead, locationtextRead, downstreamRead);
    }


}
