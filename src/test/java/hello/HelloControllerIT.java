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
		assertThat(response.getBody(), equalTo("Query 1 result"));
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
		assertThat(response.getBody(), equalTo("Query 4 result"));
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
