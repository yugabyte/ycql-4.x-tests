package com.yb.cql.test;

import static com.yugabytedb.samples.ExampleUtils.createKeyspace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.yugabyte.oss.driver.internal.core.loadbalancing.PartitionAwarePolicy;

public class TestLoadBalancingPolicy {

	protected static final Logger LOG = LoggerFactory.getLogger(TestLoadBalancingPolicy.class);

	CqlSession session = null;

	@Before
	public void setup() {

		createKeyspace();
//		session = ExampleUtils.connect();
		// Load Configuration from file
		String confFilePath = TestLoadBalancingPolicy.class.getResource("/custom_application.conf").getFile();

		// Create a Load with this file
		DriverConfigLoader loader = DriverConfigLoader.fromFile(new File(confFilePath));

		session = CqlSession.builder().withConfigLoader(loader).build();

	}

	@Test
	public void testHashFunction() throws Exception {

		Random rand = new Random();

		// Test hash key composed of strings and blob.
		{
			session.execute("create table IF NOT EXISTS t1 (h1 text, h2 text, h3 blob, h4 text, "
					+ "primary key ((h1, h2, h3, h4)));");

			// Insert a row with random hash column values.
			String h1 = RandomStringUtils.random(rand.nextInt(32), "qw32rfHIJk9iQ8Ud7h0X".toCharArray());
			String h2 = RandomStringUtils.random(rand.nextInt(256), "qw32rfHIJk9iQ8Ud7h0X".toCharArray());
			byte bytes[] = new byte[rand.nextInt(256)];
			for (int i = 0; i < bytes.length; i++) {
				bytes[i] = (byte) (rand.nextInt() & 0xff);
			}
			ByteBuffer h3 = ByteBuffer.wrap(bytes);
			String h4 = RandomStringUtils.random(rand.nextInt(256), "qw32rfHIJk9iQ8Ud7h0X".toCharArray());
			LOG.info("h1 = \"" + h1 + "\", " + "h2 = \"" + h2 + "\", " + "h3 = \"" + makeBlobString(h3) + "\", "
					+ "h4 = \"" + h4 + "\"");
			BoundStatement stmt = session.prepare("insert into t1 (h1, h2, h3, h4) values (?, ?, ?, ?);").bind(h1, h2,
					h3, h4);
			session.execute(stmt);

			// Select the row back using the hash key value and verify the row.
			BoundStatement stmt1 = session.prepare("select * from t1 where token(h1, h2, h3, h4) = ?;")
					.bind(PartitionAwarePolicy.YBToCqlHashCode(PartitionAwarePolicy.getKey(stmt)));
			Row row = session.execute(stmt1).one();
			assertNotNull(row);
			assertEquals(h1, row.getString("h1"));
			assertEquals(h2, row.getString("h2"));
			assertEquals(h3, row.getBytesUnsafe("h3").duplicate());
			assertEquals(h4, row.getString("h4"));

			session.execute("drop table t1;");
		}

		// Test hash key composed of integers and string.
		{
			session.execute("create table t2 (h1 tinyint, h2 smallint, h3 text, h4 int, h5 bigint, "
					+ "primary key ((h1, h2, h3, h4, h5)));");

			// Insert a row with random hash column values.
			byte h1 = (byte) (rand.nextInt() & 0xff);
			short h2 = (short) (rand.nextInt() & 0xffff);
			String h3 = RandomStringUtils.random(rand.nextInt(256));
			int h4 = rand.nextInt();
			long h5 = rand.nextLong();
			LOG.info("h1 = " + h1 + ", " + "h2 = " + h2 + ", " + "h3 = \"" + h3 + "\", " + "h4 = " + h4 + ", " + "h5 = "
					+ h5);
			BoundStatement stmt = session.prepare("insert into t2 (h1, h2, h3, h4, h5) " + "values (?, ?, ?, ?, ?);")
					.bind(Byte.valueOf(h1), Short.valueOf(h2), h3, Integer.valueOf(h4), Long.valueOf(h5));
			session.execute(stmt);

			// Select the row back using the hash key value and verify the row.
			BoundStatement stmt1 = session.prepare("select * from t2 where token(h1, h2, h3, h4, h5) = ?;")
					.bind(PartitionAwarePolicy.YBToCqlHashCode(PartitionAwarePolicy.getKey(stmt)));
			Row row = session.execute(stmt1).one();
			assertNotNull(row);
			assertEquals(h1, row.getByte("h1"));
			assertEquals(h2, row.getShort("h2"));
			assertEquals(h3, row.getString("h3"));
			assertEquals(h4, row.getInt("h4"));
			assertEquals(h5, row.getLong("h5"));

			session.execute("drop table t2;");
		}

		// Test hash key composed of integer, timestamp, inet, uuid and timeuuid.
		{
			session.execute("create table IF NOT EXISTS t3 (h1 int, h2 timestamp, h3 inet, h4 uuid, h5 timeuuid, "
					+ "primary key ((h1, h2, h3, h4, h5)));");

			// Insert a row with random hash column values.
			int h1 = rand.nextInt();
//	      Date h2 = new Date(rand.nextInt(Integer.MAX_VALUE));
			Instant h2 = Instant.now();
			byte addr[] = new byte[4];
			addr[0] = (byte) (rand.nextInt() & 0xff);
			addr[1] = (byte) (rand.nextInt() & 0xff);
			addr[2] = (byte) (rand.nextInt() & 0xff);
			addr[3] = (byte) (rand.nextInt() & 0xff);
			InetAddress h3 = InetAddress.getByAddress(addr);
			UUID h4 = UUID.randomUUID();
			UUID h5 = Uuids.timeBased();
			LOG.info("h1 = " + h1 + ", " + "h2 = " + h2 + ", " + "h3 = " + h3 + ", " + "h4 = " + h4 + ", " + "h5 = "
					+ h5);
			BoundStatement stmt = session.prepare("insert into t3 (h1, h2, h3, h4, h5) " + "values (?, ?, ?, ?, ?);")
					.bind(Integer.valueOf(h1), h2, h3, h4, h5);
			session.execute(stmt);

			// Select the row back using the hash key value and verify the row.
			BoundStatement stmt1 = session.prepare("select * from t3 where token(h1, h2, h3, h4, h5) = ?;")
					.bind(PartitionAwarePolicy.YBToCqlHashCode(PartitionAwarePolicy.getKey(stmt)));
			Row row = session.execute(stmt1).one();
			assertNotNull(row);
			assertEquals(h1, row.getInt("h1"));
			assertEquals(h2, row.getInstant("h2"));
			assertEquals(h3, row.getInetAddress("h3"));
			assertEquals(h4, row.getUuid("h4"));
			assertEquals(h5, row.getUuid("h5"));

			session.execute("drop table t3;");
		}

		// Test hash key composed of float and double.
		{
			session.execute("create table t4 (h1 float, h2 double, primary key ((h1, h2)));");

			// Insert a row with random hash column values.
			float h1 = rand.nextFloat() * Float.MAX_VALUE * (float) (rand.nextBoolean() ? 1.0 : -1.0);
			double h2 = rand.nextDouble() * Double.MAX_VALUE * (rand.nextBoolean() ? 1.0 : -1.0);
			LOG.info("h1 = " + h1 + ", h2 = " + h2);
			BoundStatement stmt = session.prepare("insert into t4 (h1, h2) values (?, ?);").bind(Float.valueOf(h1),
					Double.valueOf(h2));
			session.execute(stmt);

			// Select the row back using the hash key value and verify the row.
			BoundStatement stmt1 = session.prepare("select * from t4 where token(h1, h2) = ?;")
					.bind(PartitionAwarePolicy.YBToCqlHashCode(PartitionAwarePolicy.getKey(stmt)));
			Row row = session.execute(stmt1).one();
			assertNotNull(row);
			assertEquals(h1, row.getFloat("h1"), 0.0 /* delta */);
			assertEquals(h2, row.getDouble("h2"), 0.0 /* delta */);

			// Insert a row with NaN hash column values.
			h1 = Float.NaN;
			h2 = Double.NaN;
			LOG.info("h1 = " + h1 + ", h2 = " + h2);
			stmt = session.prepare("insert into t4 (h1, h2) values (?, ?);").bind(Float.valueOf(h1),
					Double.valueOf(h2));
			session.execute(stmt);

			// Select the row back using the hash key value and verify the row.
			BoundStatement stmt2 = session.prepare("select * from t4 where token(h1, h2) = ?;")
					.bind(PartitionAwarePolicy.YBToCqlHashCode(PartitionAwarePolicy.getKey(stmt)));
			row = session.execute(stmt2).one();
			assertNotNull(row);
			assertEquals(h1, row.getFloat("h1"), 0.0 /* delta */);
			assertEquals(h2, row.getDouble("h2"), 0.0 /* delta */);

			session.execute("drop table t4;");
		}

		// Test hash key composed of boolean.
		{
			session.execute("create table t7 (h boolean, primary key ((h)));");

			for (Boolean h : Arrays.asList(false, true)) {
				LOG.info("h = " + h);
				BoundStatement stmt = session.prepare("insert into t7 (h) values (?);").bind(h);
				session.execute(stmt);

				// Select the row back using the hash key value and verify the row.
				BoundStatement stmt1 = session.prepare("select * from t7 where token(h) = ?;")
						.bind(PartitionAwarePolicy.YBToCqlHashCode(PartitionAwarePolicy.getKey(stmt)));
				Row row = session.execute(stmt1).one();
				assertNotNull(row);
				assertEquals(h, row.getBoolean("h"));
			}

			session.execute("drop table t7;");
		}

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
