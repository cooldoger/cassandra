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

import org.apache.cassandra.utils.btree.BTree;
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
public class BTreeBuildBench
{
    private List<Integer> dataSingle;
    private List<Integer> dataLeaf;
    private List<Integer> data1K;
    private List<Integer> data5K;
    private List<Integer> data1M;

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

    private int buildTree(List<Integer> data)
    {
        Object[] btree = BTree.build(data, UpdateFunction.noOp());
        // access the btree to avoid java optimized out this code
        return BTree.size(btree);
    }

    private int treeBuilder(List<Integer> data)
    {
        BTree.Builder<Integer> builder = BTree.builder(Comparator.naturalOrder());
        Object[] btree = builder.addAll(data).build();
        return BTree.size(btree);
    }

    @Setup(Level.Trial)
    public void setup() throws Throwable
    {
        dataSingle = seq(1);
        dataLeaf = seq(32);
        data1K = seq(1000);
        data5K = seq(5000);
        data1M = seq(1000*1000);
    }

    @Benchmark
    public void buildSingleValueTreeTest()
    {
        int size = buildTree(dataSingle);
        assert size == 1;
    }

    @Benchmark
    public void buildLeafTreeTest()
    {
        int size = buildTree(dataLeaf);
        assert size == 32;
    }

    @Benchmark
    public void build1KValuesTreeTest()
    {
        int size = buildTree(data1K);
        assert size == 1000;
    }

    @Benchmark
    public void build5KValuesTreeTest()
    {
        int size = buildTree(data5K);
        assert size == 5000;
    }

    @Benchmark
    public void build1MValuesTreeTest()
    {
        int size = buildTree(data1M);
        assert size == 1000*1000;
    }

    @Benchmark
    public void buildSingleValueTreeBuilderTest()
    {
        int size = treeBuilder(dataSingle);
        assert size == 1;
    }

    @Benchmark
    public void buildLeafTreeBuilderTest()
    {
        int size = treeBuilder(dataLeaf);
        assert size == 32;
    }

    @Benchmark
    public void build1KValuesTreeBuilderTest()
    {
        int size = treeBuilder(data1K);
        assert size == 1000;
    }

    @Benchmark
    public void build5KValuesTreeBuilderTest()
    {
        int size = treeBuilder(data5K);
        assert size == 5000;
    }

    @Benchmark
    public void build1MValuesTreeBuilderTest()
    {
        int size = treeBuilder(data1M);
        assert size == 1000*1000;
    }

    @Benchmark
    public void transformAndFilter1KTest()
    {
        Object[] b1 = BTree.build(data1K, UpdateFunction.noOp());
        Object[] b2 = BTree.transformAndFilter(b1, (x) -> (Integer) x % 2 == 1 ? x : null);
        assert BTree.size(b1) / 2 == BTree.size(b2);
    }

    @Benchmark
    public void transformAndFilter1MTest()
    {
        Object[] b1 = BTree.build(data1M, UpdateFunction.noOp());
        Object[] b2 = BTree.transformAndFilter(b1, (x) -> (Integer) x % 2 == 1 ? x : null);
        assert BTree.size(b1) / 2 == BTree.size(b2);
    }
}
