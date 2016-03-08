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
				.andExpect(content().string(equalTo("<h1>Speeders on I-205</h1><br><table border=\"1\"><tr><td>Speed</td><td># greater than</td><td>percent of total</td></td><tr><td>50</td><td>8571269</td><td>60.9645</td></td><tr><td>60</td><td> 4412134</td><td>31.382</td></td><tr><td>70</td><td>484173</td><td>3.4438</td></td><tr><td>80</td><td>99708</td><td>0.7092</td></td><tr><td>90</td><td>30821</td><td>0.2192</td></td><tr><td>100</td><td>6972</td><td>0.0496</td></td><tr><td>102</td><td>5533</td><td>0.0394</td></td><tr><td>104</td><td>2728</td><td>0.0194</td></td><tr><td>106</td><td>279</td><td>0.002</td></td></table><br>Avg speed: 41.0 Total speed records 14059453")));
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
				.andExpect(content().string(equalTo("<h1>Route Finding: Johnson Creek NB to Columbia Blvd NB</h1><br><table border=\"1\"><tr><tr><td>Station ID</td><td>Location Text</td></tr></td><tr><tr><td>1047</td><td>Johnson Cr NB</td></tr><tr><td>1117</td><td>Foster NB</td></tr><tr><td>1048</td><td>Powell to I-205 NB</td></tr><tr><td>1142</td><td>Division NB</td></tr><tr><td>1140</td><td>Glisan to I-205 NB</td></tr>")));
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
