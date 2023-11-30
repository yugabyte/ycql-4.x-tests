package com.yb.cql.test;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.yugabytedb.samples.ExampleUtils.createKeyspace;

public class TestINClauseForListType {
    private static final int num = 100;

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
    public void testINClause() {
        String createTable = "CREATE TABLE IF NOT EXISTS employee (id int PRIMARY KEY, " + "name varchar, " +
                "age int, " + "language varchar);";
        session.execute(createTable);
        System.out.println("Executing a query now");
        for (int i = 0; i < num; i++) {
            session.execute("insert into employee (id, name, age, language) " +
                    "values (" + i + ", 'name" + i + "', " + (i+40) + ", 'address" + i + "')");
        }
        System.out.println("All inserts completed");
        PreparedStatement ps = session.prepare("select * from employee where id in ?");
        List ids = new ArrayList<Integer>();
//        Set ids = new HashSet();
        ids.add(3);
        ids.add(6);
        ids.add(9);
        ids.add(12);
        ids.add(15);
        ids.add(18);
        ids.add(21);
        BoundStatement bs = ps.bind(ids);
        ResultSet rs = session.execute(bs);

        Iterator<Row> it = rs.iterator();
        while (it.hasNext()) {
            System.out.println(it.next().getString(1));
        }
        session.close();
    }
}
