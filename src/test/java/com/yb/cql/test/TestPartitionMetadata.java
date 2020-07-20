package com.yb.cql.test;

import static com.yugabytedb.samples.ExampleUtils.createKeyspace;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Before;

//Copyright (c) YugaByte, Inc.
//
//Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
//in compliance with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software distributed under the License
//is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
//or implied.  See the License for the specific language governing permissions and limitations
//under the License.
//

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.yugabyte.oss.driver.api.core.TableSplitMetadata;
import com.yugabytedb.samples.ExampleSchema;

public class TestPartitionMetadata {
	
	protected static final Logger LOG = LoggerFactory.getLogger(TestJson.class);
	
	CqlSession session = null;
	
	@Before
	public void setup() {
		
		createKeyspace();
		// Load Configuration from file
        String confFilePath = TestLoadBalancingPolicy
                .class.getResource("/custom_application.conf").getFile();
        
        // Create a Load with this file
        DriverConfigLoader loader = 
                DriverConfigLoader.fromFile(new File(confFilePath));
        
        session = CqlSession.builder().withConfigLoader(loader).build();
		
	}

	private TableSplitMetadata getPartitionMap(TableMetadata table) {
		return session.getMetadata().getDefaultPartitionMetadata().get().getTableSplitMetadata(table.getKeyspace().toString(), table.getName().toString());
	}

	private void internalTestCreateDropTable() throws Exception {
		final int MAX_WAIT_SECONDS = 10;

		// Create test table. Verify that the PartitionMetadata gets notified of the
		// table creation
		// and loads the metadata.
		session.execute("create table test_partition1 (k int primary key);");
		TableMetadata table = session.getMetadata().getKeyspace(ExampleSchema.KEYSPACE_NAME).get()
				.getTable("test_partition1").get();
		boolean found = false;
		for (int i = 0; i < MAX_WAIT_SECONDS; i++) {
			TableSplitMetadata partitionMap = getPartitionMap(table);
			if (partitionMap != null && partitionMap.getHosts(0).size() > 0) {
				found = true;
				break;
			}
			Thread.sleep(1000);
		}
		assertTrue(found);

		// Drop test table. Verify that the PartitionMetadata gets notified of the table
		// drop
		// and clears the the metadata.
		session.execute("Drop table test_partition1;");
		for (int i = 0; i < MAX_WAIT_SECONDS; i++) {
			if (getPartitionMap(table) == null) {
				found = false;
				break;
			}
			Thread.sleep(1000);
		}
		assertFalse(found);
	}

	@Test
	public void testCreateDropTable() throws Exception {
		LOG.info("Start test: ");
		internalTestCreateDropTable();
		LOG.info("End test: ");
	}
}
