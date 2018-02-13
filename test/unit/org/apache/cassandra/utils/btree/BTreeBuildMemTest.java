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

package org.apache.cassandra.utils.btree;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;

public class BTreeBuildMemTest
{
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

    private int treeBuilderAdd(List<Integer> data)
    {
        BTree.Builder<Integer> builder = BTree.builder(Comparator.naturalOrder());
        builder.auto(false);
        for (Integer v : data)
            builder.add(v);
        Object[] btree = builder.build();
        return BTree.size(btree);
    }

    private static final JMXEnabledThreadPoolExecutor executor = new JMXEnabledThreadPoolExecutor(
            10, Integer.MAX_VALUE, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
            new NamedThreadFactory("builderTest"), "testInternal");

    @Test
    public void testBuilder() throws Throwable
    {
        List<Integer> data = seq(1000 * 1000); // 1M elements
        List<Future<?>> tasks = new ArrayList<>();
        for (int i = 0; i < 20; i++)
        {
            tasks.add(executor.submit(() ->
                                      {
                                          for (int j = 0; j < 2; j++)
                                              treeBuilderAdd(data);
                                      }));
        }
        for (Future<?> task : tasks)
            task.get();

        System.gc();
        Thread.sleep(1000 * 2);
        long total = Runtime.getRuntime().totalMemory();
        long free = Runtime.getRuntime().freeMemory();
        long used = total - free;

        //              number of elements * num of thread * element size (64bit)
        long expected = (1000 * 1000) *      10            * 8                    / 2;
        Assert.assertTrue(String.format("used heap size is larger than expected", used), used < expected);
    }
}
