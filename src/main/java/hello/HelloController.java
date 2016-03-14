package hello;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;

@RestController
public class HelloController {
    QueryNumSpeedsOver100 query1 = new QueryNumSpeedsOver100();
    QueryTotalVolumeFosterNB query2 = new QueryTotalVolumeFosterNB();
    QueryPeakTravelTimeFosterNB query3 = new QueryPeakTravelTimeFosterNB();
    QueryRouteJohnsonCreekToColumbia query4 = new QueryRouteJohnsonCreekToColumbia();
    QueryUpdateStationIDLength query5 = new QueryUpdateStationIDLength();
    Query6 query6 = new Query6();

    String SpringBootPage() {
        String ret = "<h1>HBase Queries</h1>";
        ret += "<ul>";
        ret += "<li>Query 1: <a href =\"/query1/\"> Number of speeds over 100</a></li>";
        ret += "<li>Query 2: <a href =\"/query2/\"> Total volume for Foster NB</a></li>";
        ret += "<li>Query 3: <a href =\"/query3/\"> Peak Travel Time Foster NB</a></li>";
        ret += "<li>Query 4: <a href =\"/query4/\"> Route from Johnson Creek to Columbia</a></li>";
        ret += "<li>Query 5: <a href =\"/query5/\"> Update Station ID Length</a></li>";
        ret += "</ul>";
        return ret;
    }

    @RequestMapping("/")
    public String index() {
        return SpringBootPage();
    }
    @RequestMapping("/query1/")
    public String query1() {
        return query1.getResult();
    }
    @RequestMapping("/query2/")
    public String query2() {
        return query2.getResult();
    }
    @RequestMapping("/query3/")
    public String query3() {
        return query3.getResult();
    }
    @RequestMapping("/query4/")
    public String query4() {
        return query4.getResult();
    }
    @RequestMapping("/query5/")
    public String query5() {
        return query5.getResult();
    }
    @RequestMapping("/query6/")
    public String query6() {
        return query6.getResult();
    }


}
