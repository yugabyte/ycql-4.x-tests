package com.yb.cql.test;

import static com.yugabytedb.samples.ExampleUtils.createKeyspace;
import static com.yugabytedb.samples.ExampleUtils.truncateTable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.yugabyte.oss.driver.internal.core.loadbalancing.PartitionAwarePolicy;
import com.yugabytedb.samples.ExampleUtils;

public class TestLoadBalancingPolicy {
	
protected static final Logger LOG = LoggerFactory.getLogger(TestLoadBalancingPolicy.class);
	
	CqlSession session = null;
	
	@Before
	public void setup() {
		
		createKeyspace();
//		session = ExampleUtils.connect();
		// Load Configuration from file
        String confFilePath = TestLoadBalancingPolicy
                .class.getResource("/custom_application.conf").getFile();
        
        // Create a Load with this file
        DriverConfigLoader loader = 
                DriverConfigLoader.fromFile(new File(confFilePath));
        
        session = CqlSession.builder().withConfigLoader(loader).build();
		
	}

	@Test
	public void testHashFunction() throws Exception {
		
		Random rand = new Random();
		
		// Test hash key composed of strings and blob.
	    {
	      session.execute("create table IF NOT EXISTS t1 (h1 text, h2 text, h3 blob, h4 text, " +
	                      "primary key ((h1, h2, h3, h4)));");

	      // Insert a row with random hash column values.
	      String h1 = RandomStringUtils.random(rand.nextInt(32),"qw32rfHIJk9iQ8Ud7h0X".toCharArray());
	      String h2 = RandomStringUtils.random(rand.nextInt(256),"qw32rfHIJk9iQ8Ud7h0X".toCharArray());
	      byte bytes[] = new byte[rand.nextInt(256)];
	      for (int i = 0; i < bytes.length; i++) {
	        bytes[i] = (byte)(rand.nextInt() & 0xff);
	      }
	      ByteBuffer h3 = ByteBuffer.wrap(bytes);
	      String h4 = RandomStringUtils.random(rand.nextInt(256),"qw32rfHIJk9iQ8Ud7h0X".toCharArray());
	      LOG.info("h1 = \"" + h1 + "\", " +
	               "h2 = \"" + h2 + "\", " +
	               "h3 = \"" + makeBlobString(h3) + "\", " +
	               "h4 = \"" + h4 + "\"");
	      BoundStatement stmt = session.prepare("insert into t1 (h1, h2, h3, h4) values (?, ?, ?, ?);")
	                            .bind(h1, h2, h3, h4);
	      session.execute(stmt);
	      
	      // Select the row back using the hash key value and verify the row.
	      BoundStatement stmt1 = session.prepare("select * from t1 where token(h1, h2, h3, h4) = ?;")
	    		  .bind(PartitionAwarePolicy.YBToCqlHashCode(PartitionAwarePolicy.getKey(stmt)));
	      Row row = session.execute(stmt1).one();
	      assertNotNull(row);
	      assertEquals(h1, row.getString("h1"));
	      assertEquals(h2, row.getString("h2"));
	      assertEquals(h3, row.getByte("h3"));
	      assertEquals(h4, row.getString("h4"));

	      session.execute("drop table t1;");
	    }

	}
	
	@Test
	public void testBoundStatement() throws Exception {
		
		ExampleUtils.createTableUser(session);
		truncateTable(session, ExampleUtils.USER_TABLENAME);
		
        SimpleStatement statement1 = SimpleStatement.builder("INSERT INTO users (user_id, firstname, lastname, address) "
                + "VALUES (?,?,?,?)").setConsistencyLevel(DefaultConsistencyLevel.ONE).build();
        PreparedStatement ps3 = session.prepare(statement1);
        BoundStatement bs3 = ps3.bind(new Integer(1), "Cedrick", "Lunven", "\"pqrs\"");
        session.execute(bs3);
	}
	
	private String makeBlobString(ByteBuffer buf) {
	    StringBuilder sb = new StringBuilder();
	    char[] text_values = "0123456789abcdef".toCharArray();

	    sb.append("0x");
	    while (buf.hasRemaining()) {
	      byte b = buf.get();
	      sb.append(text_values[(b & 0xFF) >>> 4]);
	      sb.append(text_values[b & 0x0F]);
	    }
	    return sb.toString();
	  }

}
