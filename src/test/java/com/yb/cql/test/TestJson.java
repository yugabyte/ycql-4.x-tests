package com.yb.cql.test;

import static com.yugabytedb.samples.ExampleUtils.createKeyspace;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.yugabytedb.samples.ExampleUtils;

public class TestJson {
	
	protected static final Logger LOG = LoggerFactory.getLogger(TestJson.class);
	
	CqlSession session = null;
	
	@Before
	public void setup() {
		
		createKeyspace();
		session = ExampleUtils.connect();
		
	}
	
	@Test
	public void testJson() throws Exception {
		
		String json =
			    "{ " +
			      "\"b\" : 1," +
			      "\"a2\" : {}," +
			      "\"a3\" : \"\"," +
			      "\"a1\" : [1, 2, 3.0, false, true, { \"k1\" : 1, \"k2\" : [100, 200, 300], \"k3\" : true}]," +
			      "\"a\" :" +
			      "{" +
			        "\"d\" : true," +
			        "\"q\" :" +
			          "{" +
			            "\"p\" : 4294967295," +
			            "\"r\" : -2147483648," +
			            "\"s\" : 2147483647" +
			          "}," +
			        "\"g\" : -100," +
			        "\"c\" : false," +
			        "\"f\" : \"hello\"," +
			        "\"x\" : 2.0," +
			        "\"y\" : 9223372036854775807," +
			        "\"z\" : -9223372036854775808," +
			        "\"u\" : 18446744073709551615," +
			        "\"l\" : 2147483647.123123e+75," +
			        "\"e\" : null" +
			      "}" +
			    "}";
		
		session.execute("CREATE TABLE IF NOT EXISTS test_json(c1 int, c2 jsonb, PRIMARY KEY(c1))");
	    session.execute(String.format("INSERT INTO test_json(c1, c2) values (1, '%s');", json));
	    session.execute("INSERT INTO test_json(c1, c2) values (2, '\"abc\"');");
	    session.execute("INSERT INTO test_json(c1, c2) values (3, '3');");
	    session.execute("INSERT INTO test_json(c1, c2) values (4, 'true');");
	    session.execute("INSERT INTO test_json(c1, c2) values (5, 'false');");
	    session.execute("INSERT INTO test_json(c1, c2) values (6, 'null');");
	    session.execute("INSERT INTO test_json(c1, c2) values (7, '2.0');");
	    session.execute("INSERT INTO test_json(c1, c2) values (8, '{\"b\" : 1}');");
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c1 = 1"));
	    
	    assertNotNull(runInvalidStmt("INSERT INTO test_json(c1, c2) values (123, abc);"));
	    runInvalidStmt("INSERT INTO test_json(c1, c2) values (123, 'abc');");
	    runInvalidStmt("INSERT INTO test_json(c1, c2) values (123, 1);");
	    runInvalidStmt("INSERT INTO test_json(c1, c2) values (123, 2.0);");
	    runInvalidStmt("INSERT INTO test_json(c1, c2) values (123, null);");
	    runInvalidStmt("INSERT INTO test_json(c1, c2) values (123, true);");
	    runInvalidStmt("INSERT INTO test_json(c1, c2) values (123, false);");
	    runInvalidStmt("INSERT INTO test_json(c1, c2) values (123, '{a:1, \"b\":2}');");

	    // Test operators.
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'p' = " +
	        "'4294967295'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'q'->>'p' = " +
	        "'4294967295'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'q'->>'p' = " +
	        "'4294967295' AND c1 = 1"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c1 = 1 AND c2->'a'->'q'->>'p' " +
	        "= '4294967295'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a1'->5->'k2'->1 = '200'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a1'->5->'k3' = 'true'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a1'->0 = '1'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a2' = '{}'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a3' = '\"\"'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'e' = 'null'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'c' = 'false'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->>'f' = 'hello'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'f' = '\"hello\"'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->>'x' = '2.000000'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'q' = " +
	        "'{\"r\": -2147483648, \"p\": 4294967295,  \"s\": 2147483647}'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->>'q' = " +
	        "'{\"p\":4294967295,\"r\":-2147483648,\"s\":2147483647}'"));

	    testScalar("\"abc\"", 2);
	    testScalar("3", 3);
	    testScalar("true", 4);
	    testScalar("false", 5);
	    testScalar("null", 6);
	    testScalar("2.0", 7);
	    assertEquals(2, session.execute("SELECT * FROM test_json WHERE c2->'b' = '1'")
	        .all().size());

	    // Test multiple where expressions.
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'g' = '-100' " +
	        "AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) = 2.0"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST (c2->'a'->>'g' as " +
	        "integer) < 0 AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) > 1.0"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST (c2->'a'->>'g' as " +
	        "integer) <= -100 AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) >= 2.0"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->>'g' as integer)" +
	        " IN (-100, -200) AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) IN (1.0, 2.0)"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->>'g' as integer)" +
	        " NOT IN (-10, -200) AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) IN (1.0, 2.0)"));

	    // Test negative where expressions.
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->'a'->'g' = '-90' " +
	        "AND c1 = 1").all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->'a'->'g' = '-90' " +
	        "AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) = 2.0").all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE CAST (c2->'a'->>'g' as " +
	        "integer) < 0 AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) < 1.0").all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE CAST (c2->'a'->>'g' as " +
	        "integer) <= -110 AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) >= 2.0").
	        all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->>'g' as integer)" +
	        " IN (-100, -200) AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) IN (1.0, 2.3)")
	        .all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->>'g' as integer)" +
	        " IN (-100, -200) AND c2->'b' = '1' AND CAST(c2->'a'->>'x' as double) NOT IN (1.0, 2.0)")
	        .all().size());

	    // Test invalid where expressions.
	    runInvalidStmt("SELECT * FROM test_json WHERE c2->'a'->'g' = '-100' AND c2 = '{}'");
	    runInvalidStmt("SELECT * FROM test_json WHERE c2 = '{} AND c2->'a'->'g' = '-100'");

	    // Test invalid operators. We should never return errors, just return an empty result (this
	    // is what postgres does).
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->'b'->'c' = '1'")
	        .all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->'z' = '1'")
	        .all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->2 = '1'")
	        .all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->'a'->2 = '1'")
	        .all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->'a1'->'b' = '1'")
	        .all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->'a1'->6 = '1'")
	        .all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->'a2'->'a' = '1'")
	        .all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->'a3'->'a' = '1'")
	        .all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->'a3'->2 = '1'")
	        .all().size());

	    // Test invalid rhs for where clause.
	    runInvalidStmt("SELECT * FROM test_json WHERE c2->'a1'->5->'k2'->1 = 200");
	    runInvalidStmt("SELECT * FROM test_json WHERE c2->'a1'->5->'k3' = true");
	    runInvalidStmt("SELECT * FROM test_json WHERE c2->'a1'->0 = 1");
	    runInvalidStmt("SELECT * FROM test_json WHERE c2->'a2' = '{a:1}'");
	    runInvalidStmt("SELECT * FROM test_json WHERE c2->'a3' = ''");
	    runInvalidStmt("SELECT * FROM test_json WHERE c2->'a'->'e' = null");
	    runInvalidStmt("SELECT * FROM test_json WHERE c2->'a'->'c' = false");
	    runInvalidStmt("SELECT * FROM test_json WHERE c2->'a'->>'f' = hello");

	    // Test json operators in select clause.
	    assertEquals("4294967295",
	        session.execute(
	            "SELECT c2->'a'->'q'->>'p' FROM test_json WHERE c1 = 1").one().getString(0));
	    assertEquals("200",
	        session.execute(
	            "SELECT c2->'a1'->5->'k2'->1 FROM test_json WHERE c1 = 1").one().getString(0));
	    assertEquals("true",
	        session.execute(
	            "SELECT c2->'a1'->5->'k3' FROM test_json WHERE c1 = 1").one().getString(0));
	    assertEquals("2.000000",
	        session.execute(
	            "SELECT c2->'a'->>'x' FROM test_json WHERE c1 = 1").one().getString(0));
	    assertEquals("{\"p\":4294967295,\"r\":-2147483648,\"s\":2147483647}",
	        session.execute(
	            "SELECT c2->'a'->'q' FROM test_json WHERE c1 = 1").one().getString(0));
	    assertEquals("{\"p\":4294967295,\"r\":-2147483648,\"s\":2147483647}",
	        session.execute(
	            "SELECT c2->'a'->>'q' FROM test_json WHERE c1 = 1").one().getString(0));
	    assertEquals("\"abc\"",
	        session.execute(
	            "SELECT c2 FROM test_json WHERE c1 = 2").one().getString(0));
	    assertEquals("true",
	        session.execute(
	            "SELECT c2 FROM test_json WHERE c1 = 4").one().getString(0));
	    assertEquals("false",
	        session.execute(
	            "SELECT c2 FROM test_json WHERE c1 = 5").one().getString(0));

	    // Json operators in both select and where clause.
	    assertEquals("4294967295",
	        session.execute(
	            "SELECT c2->'a'->'q'->>'p' FROM test_json WHERE c2->'a1'->5->'k2'->1 = '200'").one()
	            .getString(0));
	    assertEquals("{\"p\":4294967295,\"r\":-2147483648,\"s\":2147483647}",
	        session.execute(
	            "SELECT c2->'a'->'q' FROM test_json WHERE c2->'a1'->5->'k3' = 'true'").one()
	            .getString(0));

	    // JSON expression is now named as its content instead of "expr".
	    // If users actually rely on "expr", we have to change it back.
	    assertEquals("{\"p\":4294967295,\"r\":-2147483648,\"s\":2147483647}",
	        session.execute(
	            "SELECT c2->'a'->'q' FROM test_json WHERE c2->'a1'->5->'k3' = 'true'").one()
	            .getString("c2->'a'->'q'"));

	    // Test select with invalid operators, which should result in empty rows.
	    verifyEmptyRows(session.execute("SELECT c2->'b'->'c' FROM test_json WHERE c1 = 1"), 1);
	    verifyEmptyRows(session.execute("SELECT c2->'z' FROM test_json"), 8);
	    verifyEmptyRows(session.execute("SELECT c2->2 FROM test_json"), 8);
	    verifyEmptyRows(session.execute("SELECT c2->'a'->2 FROM test_json"), 8);
	    verifyEmptyRows(session.execute("SELECT c2->'a1'->'b' FROM test_json"), 8);
	    verifyEmptyRows(session.execute("SELECT c2->'a1'->6 FROM test_json"), 8);
	    verifyEmptyRows(session.execute("SELECT c2->'a1'->'a' FROM test_json"), 8);
	    verifyEmptyRows(session.execute("SELECT c2->'a3'->'a' FROM test_json"), 8);
	    verifyEmptyRows(session.execute("SELECT c2->'a3'->2 FROM test_json"), 8);

	    // Test casts.
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
	        "bigint) = 4294967295"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
	        "decimal) = 4294967295"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
	        "text) = '4294967295'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'r' as " +
	        "integer) = -2147483648"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'r' as " +
	        "text) = '-2147483648'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'s' as " +
	        "integer) = 2147483647"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a1'->5->'k2'->>1 as " +
	        "integer) = 200"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->>'x' as float) =" +
	        " 2.0"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->>'x' as double) " +
	        "= 2.0"));

	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
	        "bigint) >= 4294967295"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
	        "bigint) > 100"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
	        "bigint) <= 4294967295"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
	        "bigint) < 4294967297"));

	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
	        "bigint) >= 4294967297").all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
	        "bigint) > 4294967298").all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
	        "bigint) = 100").all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
	        "bigint) < 99").all().size());
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as " +
	        "decimal) < 99").all().size());

	    // Invalid cast types.
	    runInvalidStmt("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as boolean) = 123");
	    runInvalidStmt("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as inet) = 123");
	    runInvalidStmt("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as map) = 123");
	    runInvalidStmt("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as set) = 123");
	    runInvalidStmt("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as list) = 123");
	    runInvalidStmt("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as timestamp) = 123");
	    runInvalidStmt("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as timeuuid) = 123");
	    runInvalidStmt("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as uuid) = 123");
	    runInvalidStmt("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->>'p' as varint) = 123");
	    runInvalidStmt("SELECT * FROM test_json WHERE CAST(c2->'a'->'q'->'p' as text) = '123'");

	    // Test update.
	    session.execute("UPDATE test_json SET c2->'a'->'q'->'p' = '100' WHERE c1 = 1");
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'p' = " +
	        "'100'"));
	    session.execute("UPDATE test_json SET c2->'a'->'q'->'p' = '\"100\"' WHERE c1 = 1 IF " +
	        "c2->'a'->'q'->'s' = '2147483647' AND c2->'a'->'q'->'r' = '-2147483648'");
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'p' = " +
	        "'\"100\"'"));
	    session.execute("UPDATE test_json SET c2->'a1'->5->'k2'->2 = '2000' WHERE c1 = 1");
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a1'->5->'k2'->2 = " +
	        "'2000'"));
	    session.execute("UPDATE test_json SET c2->'a2' = '{\"x1\": 1, \"x2\": 2, \"x3\": 3}' WHERE c1" +
	        " = 1");
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a2' = " +
	        "'{\"x1\": 1, \"x2\": 2, \"x3\": 3}'"));
	    session.execute("UPDATE test_json SET c2->'a'->'e' = '{\"y1\": 1, \"y2\": {\"z1\" : 1}}' " +
	        "WHERE c1 = 1");
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'e' = " +
	        "'{\"y1\": 1, \"y2\": {\"z1\" : 1}}'"));

	    // Test updates that don't apply.
	    session.execute("UPDATE test_json SET c2->'a'->'q'->'p' = '\"200\"' WHERE c1 = 1 IF " +
	        "c2->'a'->'q'->'s' = '2' AND c2->'a'->'q'->'r' = '-2147483648'");
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'p' = " +
	        "'\"200\"'").all().size());

	    // Invalid updates.
	    // Invalid rhs (needs to be valid json)
	    runInvalidStmt("UPDATE test_json SET c2->'a'->'q'->'p' = 100 WHERE c1 = 1");

	    // Interpret update of missing entry as insert when LHS path has only one extra hop.
	    session.execute("UPDATE test_json SET c2->'a'->'q'->'xyz' = '100' WHERE c1 = 1");
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'xyz' = " +
	          "'100'"));

	    // Interpret update of missing entry as insert when LHS path has only one extra hop -
	    // allow subtree to be inserted.
	    String subtree =
	      "{ " +
	        "\"def\" : " +
	          "{ " +
	            "\"ghi\" : 100," +
	            "\"jkl\" : 200" +
	          "}" +
	      "}";
	    session.execute(String.format(
	          "UPDATE test_json SET c2->'a'->'q'->'abc' = '%s' WHERE c1 = 1", subtree));
	    verifyResultSet(session.execute(
	          "SELECT * FROM test_json WHERE c2->'a'->'q'->'abc'->'def'->'ghi' = '100'"));
	    verifyResultSet(session.execute(
	          "SELECT * FROM test_json WHERE c2->'a'->'q'->'abc'->'def'->'jkl' = '200'"));

	    // Non-existent key - cannot interpret update of missing entry as insert when LHS path has
	    // multiple extra hops.
	    runInvalidStmt("UPDATE test_json SET c2->'aa'->'q'->'p' = '100' WHERE c1 = 1");

	    // Array out of bounds.
	    runInvalidStmt("UPDATE test_json SET c2->'a1'->200->'k2'->2 = '2000' WHERE c1 = 1");
	    runInvalidStmt("UPDATE test_json SET c2->'a1'->-2->'k2'->2 = '2000' WHERE c1 = 1");
	    runInvalidStmt("UPDATE test_json SET c2->'a1'->5->'k2'->100 = '2000' WHERE c1 = 1");
	    runInvalidStmt("UPDATE test_json SET c2->'a1'->5->'k2'->-1 = '2000' WHERE c1 = 1");
	    // Mixup arrays and objects.
	    runInvalidStmt("UPDATE test_json SET c2->'a'->'q'->1 = '100' WHERE c1 = 1");
	    runInvalidStmt("UPDATE test_json SET c2->'a1'->5->'k2'->'abc' = '2000' WHERE c1 = 1");
	    runInvalidStmt("UPDATE test_json SET c2->5->'q'->'p' = '100' WHERE c1 = 1");
	    runInvalidStmt("UPDATE test_json SET c2->'a1'->'b'->'k2'->2 = '2000' WHERE c1 = 1");
	    // Invalid RHS.
	    runInvalidStmt("UPDATE test_json SET c2->'a'->'q'->'p' = c1->'a' WHERE c1 = 1");
	    runInvalidStmt("UPDATE test_json SET c2->'a'->'q'->>'p' = c2->>'b' WHERE c1 = 1");


	    // Update the same column multiple times.
	    session.execute("UPDATE test_json SET c2->'a'->'q'->'r' = '200', c2->'a'->'x' = '2', " +
	        "c2->'a'->'l' = '3.0' WHERE c1 = 1");
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'r' = '200'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'x' = '2'"));
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'l' = '3.0'"));

	    // Can't set entire column and nested attributes at the same time.
	    runInvalidStmt("UPDATE test_json SET c2->'a'->'q'->'r' = '200', c2 = '{a : 1, b: 2}' WHERE c1" +
	        " = 1");
	    // Subscript args with json not allowed.
	    runInvalidStmt("UPDATE test_json SET c2->'a'->'q'->'r' = '200', c2[0] = '1' WHERE c1 = 1");

	    // Test delete with conditions.
	    // Test deletes that don't apply.
	    session.execute("DELETE FROM test_json WHERE c1 = 1 IF " +
	        "c2->'a'->'q'->'s' = '200' AND c2->'a'->'q'->'r' = '-2147483648'");
	    verifyResultSet(session.execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'p' = " +
	        "'\"100\"'"));

	    // Test delete that applies.
	    session.execute("DELETE FROM test_json WHERE c1 = 1 IF " +
	        "c2->'a'->'q'->'s' = '2147483647' AND c2->'a'->'q'->'r' = '200'");
	    assertEquals(0, session.execute("SELECT * FROM test_json WHERE c2->'a'->'q'->'p' = " +
	        "'\"100\"'").all().size());
	    
		
	}

	private void verifyResultSet(ResultSet rs) {
		List<Row> rows = rs.all();
		assertEquals(1, rows.size());
		Row row = rows.get(0);
		JSONObject jsonObject = new JSONObject(row.getString("c2"));
		assertEquals(1, jsonObject.getInt("b"));
		assertEquals(false, jsonObject.getJSONArray("a1").getBoolean(3));
		assertEquals(3.0, jsonObject.getJSONArray("a1").getDouble(2), 1e-9);
		assertEquals(200, jsonObject.getJSONArray("a1").getJSONObject(5).getJSONArray("k2").getInt(1));
		assertEquals(2147483647, jsonObject.getJSONObject("a").getJSONObject("q").getInt("s"));
		assertEquals("hello", jsonObject.getJSONObject("a").getString("f"));
	}

	private void testScalar(String json, int c1) {
		Row row = session.execute(String.format("SELECT * FROM test_json WHERE c2 = '%s'", json)).one();
		assertEquals(c1, row.getInt("c1"));
		assertEquals(json, row.getString("c2"));
	}

	private void verifyEmptyRows(ResultSet rs, int expected_rows) {
		List<Row> rows = rs.all();
		assertEquals(expected_rows, rows.size());
		for (Row row : rows) {
			assertTrue(row.isNull(0));
		}
	}
	
	protected String runInvalidStmt(String stmt) {
		return runInvalidStmt(stmt, session);
	}

	protected String runInvalidStmt(String stmt, CqlSession s) {
		return runInvalidStmt(SimpleStatement.newInstance(stmt), s);
	}

	protected String runInvalidStmt(@SuppressWarnings("rawtypes") Statement stmt, CqlSession s) {
		QueryValidationException exception;
		try {
			s.execute(stmt);
			fail(String.format("Statement did not fail: %s", stmt));
			return null; // Never happens, but keeps compiler happy
		} catch (QueryValidationException qv) {
			exception = qv;
			LOG.info("Expected exception", qv);
		} 
		return exception.getMessage();
	}

}
