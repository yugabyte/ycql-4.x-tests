package com.yb.cql.test;

import static com.yugabytedb.samples.ExampleUtils.createKeyspace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yb.minicluster.BaseMiniClusterTest;
import org.yb.minicluster.IOMetrics;
import org.yb.minicluster.MiniYBDaemon;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;

public class TestDMLRoutingLocality extends BaseMiniClusterTest {

	protected static final Logger LOG = LoggerFactory.getLogger(TestLoadBalancingPolicy.class);

	CqlSession session = null;

	@Before
	public void setup() {

		createKeyspace();

		String confFilePath = TestLoadBalancingPolicy.class.getResource("/custom_application.conf").getFile();

		// Create a Load with this file
		DriverConfigLoader loader = DriverConfigLoader.fromFile(new File(confFilePath));

		session = CqlSession.builder().withConfigLoader(loader).build();

	}
	
	// Test load-balancing policy with DMLs.
		@Test
		public void testDML() throws Exception {

			final int NUM_KEYS = 100;

			// Create test table.
			session.execute("create table test_lb (h1 int, h2 text, c int, primary key ((h1, h2)));");

			waitForMetadataRefresh();

			// Get the initial metrics.
			Map<MiniYBDaemon, IOMetrics> initialMetrics = getTSMetrics();

			PreparedStatement stmt;

			stmt = session.prepare("insert into test_lb (h1, h2, c) values (?, ?, ?);");
			for (int i = 1; i <= NUM_KEYS; i++) {
				session.execute(stmt.bind(Integer.valueOf(i), "v" + i, Integer.valueOf(i)));
			}

			stmt = session.prepare("update test_lb set c = ? where h1 = ? and h2 = ?;");
			for (int i = 1; i <= NUM_KEYS; i++) {
				session.execute(stmt.bind(Integer.valueOf(i * 2), Integer.valueOf(i), "v" + i));
			}

			stmt = session.prepare("select c from test_lb where h1 = ? and h2 = ?;");
			for (int i = 1; i <= NUM_KEYS; i++) {
				Row row = session.execute(stmt.bind(Integer.valueOf(i), "v" + i)).one();
				assertNotNull(row);
				assertEquals(i * 2, row.getInt("c"));
			}

			stmt = session.prepare("delete from test_lb where h1 = ? and h2 = ?;");
			for (int i = 1; i <= NUM_KEYS; i++) {
				session.execute(stmt.bind(Integer.valueOf(i), "v" + i));
			}

			// Check the metrics again.
			IOMetrics totalMetrics = getCombinedMetrics(initialMetrics);

			// Verify that the majority of read and write calls are local.
			//
			// With PartitionAwarePolicy, all calls should be local ideally but there is no
			// 100% guarantee
			// because as soon as the test table has been created and the partition metadata
			// has been
			// loaded, the cluster's load-balancer may still be rebalancing the leaders.
			assertTrue("Local Read Count: " + totalMetrics.localReadCount, totalMetrics.localReadCount >= NUM_KEYS * 0.7);
			assertTrue("Local Write Count: " + totalMetrics.localWriteCount,
					totalMetrics.localWriteCount >= NUM_KEYS * 3 * 0.7);
		}
		
		private void waitForMetadataRefresh() throws Exception {
			// Since partition metadata is refreshed asynchronously after a new table is
			// created, let's
			// wait for a little or else the initial statements will be executed without the
			// partition
			// metadata and will be dispatched to a random node.
			Thread.sleep(10000);
		}

}
