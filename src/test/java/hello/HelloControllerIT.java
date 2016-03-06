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
		assertThat(response.getBody(), equalTo("<h1>Speeders on I205</h1><br>Speeds over 70: 484173\tPercentage of total: 3.4438%<br>Speeds over 80: 99708\tPercentage of total: 0.7092%<br>Speeds over 90: 30821\tPercentage of total: 0.2192%<br>Speeds over 100: 6972\tPercentage of total: 0.0496%<br>Speeds over 102: 5533\tPercentage of total: 0.0394%<br>Speeds over 104: 2728\tPercentage of total: 0.0194%<br>Speeds over 106: 279\tPercentage of total: 0.002%<br><br>Avg speed: 41.0 Total speed records 14059453"));
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
		assertThat(response.getBody(), equalTo("1047 Johnson Cr NB<br>1117 Foster NB<br>1048 Powell to I-205 NB<br>1142 Division NB<br>1140 Glisan to I-205 NB<br>1140 Columbia to I-205 NB"));
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
