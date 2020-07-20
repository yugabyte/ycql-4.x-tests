package com.yb.cql.test;

import static com.yugabytedb.samples.ExampleUtils.createKeyspace;
import static com.yugabytedb.samples.ExampleUtils.truncateTable;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.yugabytedb.samples.ExampleUtils;

public class TestBoundStatement {
	
	protected static final Logger LOG = LoggerFactory.getLogger(TestBoundStatement.class);
	
	CqlSession session = null;
	
	@Before
	public void setup() {
		createKeyspace();
        String confFilePath = TestLoadBalancingPolicy
                .class.getResource("/custom_application.conf").getFile();
        
        // Create a Load with this file
        DriverConfigLoader loader = 
                DriverConfigLoader.fromFile(new File(confFilePath));
        
        session = CqlSession.builder().withConfigLoader(loader).build();
	}
	
	@Test
	public void testBoundStatementWithIntKey() throws Exception {
		
		ExampleUtils.createTableUser(session);
		truncateTable(session, ExampleUtils.USER_TABLENAME);
		
        SimpleStatement statement1 = SimpleStatement.builder("INSERT INTO users (user_id, firstname, lastname, address) "
                + "VALUES (?,?,?,?)").setConsistencyLevel(DefaultConsistencyLevel.ONE).build();
        PreparedStatement ps3 = session.prepare(statement1);
        BoundStatement bs3 = ps3.bind(new Integer(1), "Cedrick", "Lunven", "\"pqrs\"");
        session.execute(bs3);
	}
	

}
