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

package org.apache.cassandra.test.microbench;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.Random;

import org.apache.cassandra.utils.btree.BTreeSearchIterator;
import org.apache.cassandra.utils.btree.BTree;
import org.apache.cassandra.utils.btree.BTree.Dir;
import org.apache.cassandra.utils.btree.UpdateFunction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(1)
@State(Scope.Benchmark)
public class BTreeSearchIteratorBench
{
    @Param({"1", "16", "32", "100", "1000", "10000", "100000"})
    private int btreeSize;

    @Param({"36", "100", "1000", "10000"})
    private int cellSize;

    private Object[] btree;
    private ArrayList<String> data;

    private static ArrayList<String> seq(int count, int minCellSize)
    {
        int len = 1;
        ArrayList<String> ret = new ArrayList<>();
        for (int i = 0 ; i < count ; i++)
        {
            StringBuilder sb = new StringBuilder();
            while (sb.length() < minCellSize)
            {
                String uuid = UUID.randomUUID().toString();
                sb.append(uuid);
            }
            ret.add(sb.toString());
        }
        Collections.sort(ret);
        return ret;
    }

    private static final Comparator<String> CMP = new Comparator<String>()
    {
        public int compare(String s1, String s2)
        {
            return s1.compareTo(s2);
        }
    };

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        data = seq(btreeSize, cellSize);
        btree = BTree.build(data, UpdateFunction.noOp());
    }

    @Benchmark
    public void searchFound()
    {
        BTreeSearchIterator<String, String> iter = BTree.slice(btree, CMP, Dir.ASC);
        Random rand = new Random(2);
        String val = iter.next(data.get(rand.nextInt(btreeSize)));
        assert(val != null);
    }

    @Benchmark
    public void searchNotFound()
    {
        BTreeSearchIterator<String, String> iter = BTree.slice(btree, CMP, Dir.ASC);
        String uuid = UUID.randomUUID().toString();
        String val = iter.next(uuid);
        if (val != null)
            System.out.println("WOOOOOOO uuid collision ^_^!");
    }

    @Benchmark
    public void iteratorTree()
    {
        BTreeSearchIterator<String, String> iter = BTree.slice(btree, CMP, Dir.ASC);
        String uuid = UUID.randomUUID().toString();
        while(iter.hasNext())
        {
            if (uuid.equals(iter.next()))
            {
                System.out.println("WOOOOOOO uuid collision ^_^!");
            }
        }
    }
}
