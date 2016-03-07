package hello;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import java.net.URL;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.IntegrationTest;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.TestRestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.client.RestTemplate;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = Application.class)
@WebAppConfiguration
@IntegrationTest({"server.port=0"})
public class HelloControllerIT {

    @Value("${local.server.port}")
    private int port;

	private URL base;
	private RestTemplate template;

	@Before
	public void setUp() throws Exception {
		this.base = new URL("http://localhost:" + port + "/");
		template = new TestRestTemplate();
	}

	@Test
	public void getHello() throws Exception {
		ResponseEntity<String> response = template.getForEntity(base.toString(), String.class);
		assertThat(response.getBody(), equalTo("Hello World w/Spring Boot"));
	}
	@Test
	public void getQuery1() throws Exception {
		ResponseEntity<String> response = template.getForEntity(base.toString()  + "query1/", String.class);
		assertThat(response.getBody(), equalTo("<h1>Speeders on I-205</h1><br><table border=\"1\"><tr><td>Speed</td><td># greater than</td><td>percent of total</td></td><tr><td>50</td><td>8571269</td><td>60.9645</td></td><tr><td>60</td><td> 4412134</td><td>31.382</td></td><tr><td>70</td><td>484173</td><td>3.4438</td></td><tr><td>80</td><td>99708</td><td>0.7092</td></td><tr><td>90</td><td>30821</td><td>0.2192</td></td><tr><td>100</td><td>6972</td><td>0.0496</td></td><tr><td>102</td><td>5533</td><td>0.0394</td></td><tr><td>104</td><td>2728</td><td>0.0194</td></td><tr><td>106</td><td>279</td><td>0.002</td></td></table><br>Avg speed: 41.0 Total speed records 14059453"));
	}
	@Test
	public void getQuery2() throws Exception {
		ResponseEntity<String> response = template.getForEntity(base.toString() + "query2/", String.class);
		assertThat(response.getBody(), equalTo("Query 2 result"));
	}
	@Test
	public void getQuery3() throws Exception {
		ResponseEntity<String> response = template.getForEntity(base.toString() + "query3/", String.class);
		assertThat(response.getBody(), equalTo("Query 3 result"));
	}
	@Test
	public void getQuery4() throws Exception {
		ResponseEntity<String> response = template.getForEntity(base.toString() + "query4/", String.class);
		assertThat(response.getBody(), equalTo("<h1>Route Finding: Johnson Creek NB to Columbia Blvd NB</h1><br><table border=\"1\"><tr><td>Station ID</td><td>Location Text</td></td><tr><td>1046</td><td>Johnson Cr NB</td><br><td>1047</td><td>Johnson Cr NB</td><br><td>1117</td><td>Foster NB</td><br><td>1048</td><td>Powell to I-205 NB</td><br><td>1142</td><td>Division NB/td><br><td>1140</td><td>Columbia to I-205 NB</td>"));
	}
	@Test
	public void getQuery5() throws Exception {
		ResponseEntity<String> response = template.getForEntity(base.toString() + "query5/", String.class);
		assertThat(response.getBody(), equalTo("Query 5 result"));
	}
	@Test
	public void getQuery6() throws Exception {
		ResponseEntity<String> response = template.getForEntity(base.toString()  + "query6/", String.class);
		assertThat(response.getBody(), equalTo("Query 6 result"));
	}
}
