package hello;

import static org.hamcrest.Matchers.equalTo;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockServletContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = MockServletContext.class)
@WebAppConfiguration
public class HelloControllerTest {

	private MockMvc mvc;

	@Before
	public void setUp() throws Exception {
		mvc = MockMvcBuilders.standaloneSetup(new HelloController()).build();
	}

	@Test
	public void getHello() throws Exception {
		mvc.perform(MockMvcRequestBuilders.get("/").accept(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk())
				.andExpect(content().string(equalTo("Hello World w/Spring Boot")));
	}
	@Test
	public void getQuery1() throws Exception {
		mvc.perform(MockMvcRequestBuilders.get("/query1/").accept(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk())
				.andExpect(content().string(equalTo("Speeds over 70: 484173 percentage of total: 3.4437541773495743%<br>Speeds over 80: 99708 percentage of total: 0.7091883304421587%<br>Speeds over 90: 30821 percentage of total: 0.2192190549660787%<br>Speeds over 100: 6972 percentage of total: 0.04958941147994876%<br>Speeds over 102: 5533 percentage of total: 0.039354304893654116%<br>Speeds over 104: 2728 percentage of total: 0.019403315335240992%<br>Speeds over 106: 279 percentage of total: 0.0019844299774678287%<br><br>Avg speed: 41.0 Total speed records 14059453")));
	}
	@Test
	public void getQuery2() throws Exception {
		mvc.perform(MockMvcRequestBuilders.get("/query2/").accept(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk())
				.andExpect(content().string(equalTo("Query 2 result")));
	}
	@Test
	public void getQuery3() throws Exception {
		mvc.perform(MockMvcRequestBuilders.get("/query3/").accept(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk())
				.andExpect(content().string(equalTo("Query 3 result")));
	}
	@Test
	public void getQuery4() throws Exception {
		mvc.perform(MockMvcRequestBuilders.get("/query4/").accept(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk())
				.andExpect(content().string(equalTo("1047 Johnson Cr NB<br>1117 Foster NB<br>1048 Powell to I-205 NB<br>1142 Division NB<br>1140 Glisan to I-205 NB<br>1140 Columbia to I-205 NB")));
	}
	@Test
	public void getQuery5() throws Exception {
		mvc.perform(MockMvcRequestBuilders.get("/query5/").accept(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk())
				.andExpect(content().string(equalTo("Query 5 result")));
	}
	@Test
	public void getQuery6() throws Exception {
		mvc.perform(MockMvcRequestBuilders.get("/query6/").accept(MediaType.APPLICATION_JSON))
				.andExpect(status().isOk())
				.andExpect(content().string(equalTo("Query 6 result")));
	}
}
