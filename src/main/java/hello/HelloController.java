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

    @RequestMapping("/")
    public String index() {
        return "Hello World w/Spring Boot";
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
