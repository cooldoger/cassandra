/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.transport.Server;

public class ComparisonTest extends CQLTester
{
    int cqlVersion = Server.VERSION_3;

    @BeforeClass
    public static void setUp()
    {
        // This line is needed in 2.2, not in 3.0
        DatabaseDescriptor.setPartitioner(ByteOrderedPartitioner.instance);
    }

    @Test
    public void testCQL() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 int, v int, PRIMARY KEY(k, c1, c2)) WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC)");

        execute("INSERT INTO %s(k, c1, c2, v) VALUES (?, ?, ?, ?)", 1, 10, 0, 1);
        execute("INSERT INTO %s(k, c1, c2, v) VALUES (?, ?, ?, ?)", 1, 10, 10, 2);
        execute("INSERT INTO %s(k, c1, c2, v) VALUES (?, ?, ?, ?)", 1, 1, 0, 3);
        execute("INSERT INTO %s(k, c1, c2, v) VALUES (?, ?, ?, ?)", 1, 1, 10, 4);

        com.datastax.driver.core.ResultSet res = executeNet(cqlVersion, "SELECT * from %s WHERE k = 1 AND (c1, c2) >= (? , ?) AND (c1, c2) <= (?, ?)", 1, 10, 10, 0);

        System.out.println(res.all());
    }
}
