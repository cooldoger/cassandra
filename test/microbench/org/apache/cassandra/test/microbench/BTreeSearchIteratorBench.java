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
import java.util.Comparator;
import java.util.List;
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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 20, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 2)
@Threads(1)
@State(Scope.Benchmark)
public class BTreeSearchIteratorBench
{
    private int sum;
    private Object[] btreeBig;  // 1000 nodes
    private Object[] btreeLeaf; // 32 nodes leaf
    private Object[] btreeOneElem; // only one node

    private static List<Integer> seq(int count)
    {
        return seq(count, 0, 1);
    }

    private static List<Integer> seq(int count, int base, int multi)
    {
        List<Integer> r = new ArrayList<>();
        for (int i = 0 ; i < count ; i++)
            r.add(i * multi + base);
        return r;
    }

    private static final Comparator<Integer> CMP = new Comparator<Integer>()
    {
        public int compare(Integer o1, Integer o2)
        {
            return Integer.compare(o1, o2);
        }
    };

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        btreeBig = BTree.build(seq(1000, 0, 2), UpdateFunction.noOp());
        btreeLeaf = BTree.build(seq(32, 0, 2), UpdateFunction.noOp());
        btreeOneElem = BTree.build(seq(1), UpdateFunction.noOp());
        sum = 0;
    }

    @Benchmark
    public void searchBigTreeTest()
    {
        BTreeSearchIterator<Integer, Integer> iter = BTree.slice(btreeBig, CMP, Dir.ASC);
        Random rand = new Random(2);
        Integer val = iter.next(rand.nextInt(1000) * 2);
        assert(val != null);
        sum += val;
    }

    @Benchmark
    public void searchNotFoundBigTreeTest()
    {
        BTreeSearchIterator<Integer, Integer> iter = BTree.slice(btreeBig, CMP, Dir.ASC);
        Random rand = new Random(2);
        Integer val = iter.next(rand.nextInt(1000) * 2 + 1);
        assert(val == null);
        if (val != null) {
            throw new RuntimeException("Should not find any result:" + val.toString());
        }
    }

    @Benchmark
    public void iteratorBigTreeTest()
    {
        BTreeSearchIterator<Integer, Integer> iter = BTree.slice(btreeBig, CMP, Dir.ASC);
        while(iter.hasNext())
        {
            sum += iter.next();
        }
    }

    @Benchmark
    public void searchFoundLeafTreeTest()
    {
        BTreeSearchIterator<Integer, Integer> iter = BTree.slice(btreeLeaf, CMP, Dir.ASC);
        Random rand = new Random(2);
        Integer val = iter.next(rand.nextInt(32) * 2);
        assert(val != null);
        sum += val;
    }

    @Benchmark
    public void searchNotFoundLeafTreeTest()
    {
        BTreeSearchIterator<Integer, Integer> iter = BTree.slice(btreeLeaf, CMP, Dir.ASC);
        Random rand = new Random(2);
        Integer val = iter.next(rand.nextInt(32) * 2 + 1);
        assert(val == null);
        if (val != null) {
            throw new RuntimeException("Should not find any result: " + val.toString());
        }
    }

    @Benchmark
    public void iteratorLeafTreeTest()
    {
        BTreeSearchIterator<Integer, Integer> iter = BTree.slice(btreeLeaf, CMP, Dir.ASC);
        while(iter.hasNext())
        {
            sum += iter.next();
        }
    }

    @Benchmark
    public void searchFoundOneElemTreeTest()
    {
        BTreeSearchIterator<Integer, Integer> iter = BTree.slice(btreeOneElem, CMP, Dir.ASC);
        Integer val = iter.next(0);
        assert(val != null);
        sum += val;
    }

    @Benchmark
    public void searchNotFoundOneElemTreeTest()
    {
        BTreeSearchIterator<Integer, Integer> iter = BTree.slice(btreeOneElem, CMP, Dir.ASC);
        Integer val = iter.next(1);
        assert(val == null);
        if (val != null) {
            throw new RuntimeException("Should not find any result: " + val.toString());
        }
    }

    @Benchmark
    public void iteratorOneElemTreeTest()
    {
        BTreeSearchIterator<Integer, Integer> iter = BTree.slice(btreeOneElem, CMP, Dir.ASC);
        while(iter.hasNext())
        {
            sum += iter.next();
        }
    }
}
