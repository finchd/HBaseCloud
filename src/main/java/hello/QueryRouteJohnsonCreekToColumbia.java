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
    private Pair<String, String> endingStation;
    private Pair<String, String> nextStation;
    private Pair<String, String> currentStation;
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

                nextStation = getNextStationIdFromDownstream(Integer.parseInt(currentStation.getKey()));

                // matches endingStation.  return result
                if (nextStation.getKey().equals(endingStation.getKey())) {

                    theResult = theResult + "\n" + nextStation.getKey() + nextStation.getValue() + "\n";
                    return theResult;

                }
                // nextStation = 0; no route found.
                else if (Integer.parseInt(nextStation.getKey()) == 0) {
                    theResult = "No route found.";
                    return theResult;
                }
                // add intermediate station result to theResult string
                else {
                    theResult = theResult + "\n" + nextStation.getKey() + nextStation.getValue() + "\n";
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
        Pair<String, String> startingStation = getStationIdFromLocationText(startingLocationText);
        currentStation = startingStation;
        endingStation = getStationIdFromLocationText(endingLocationText);
        nextStation = getNextStationIdFromDownstream(Integer.parseInt(startingStation.getKey()));
    }

    /**
     * Generic Pair class, to hold our stationid, locationtext pairs
     * @param <K> The stationid
     * @param <V> The locationtext
     */
    class Pair<K, V> {
        private K k;
        private V v;

        public Pair(K k, V v) {
            this.k = k;
            this.v = v;
        }
        public K getKey() {
            return k;
        }
        public V getValue() {
            return v;
        }
    }


    /**
     * Get the station pair that corresponds to the matching the locationtext
     * @param locationtext the locationtext to match
     * @return the corresponding (stationid, locationtext) pair
     */
    public Pair<String,String> getStationIdFromLocationText(String locationtext) {
        Result result;
        String locationtextRead;
        String downstreamRead;
        String stationidRead;
        Pair<String, String> station;

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
                        station = new Pair<>(stationidRead, locationtextRead);
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

        return new Pair<>(null, null);
    }

    /**
     * Get the next downstream station id
     * @param downstream the downstream stationid
     * @return The (stationid, locationtext) pair corresponding to the downstream stationid
     */
    public Pair<String,String> getNextStationIdFromDownstream(int downstream) {
        Result result = null;
        String locationtextRead = null;
        String downstreamRead = null;

        try {
            Get theGet = new Get(Bytes.toBytes(String.valueOf(downstream)));
            result = table.get(theGet);
        }
        catch (java.io.IOException e) {
            e.printStackTrace();
        }

        if (result != null) {
            // Get the next downstream's locationtext
            byte[] locationtextByte = result.getValue(Bytes.toBytes("freeway_stations"), Bytes.toBytes("locationtext"));
            locationtextRead = Bytes.toString(locationtextByte);
            // Get the next downstream
            byte[] downstreamByte = result.getValue(Bytes.toBytes("freeway_stations"), Bytes.toBytes("downstream"));
            downstreamRead = Bytes.toString(downstreamByte);

        }

        return new Pair<>(downstreamRead, locationtextRead);
    }


}
