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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.cassandra.utils.IndexedSearchIterator;
import org.apache.cassandra.utils.btree.BTree.Dir;
import org.junit.Test;

public class BTreeSearchIteratorTest
{
    private static List<Integer> seq(int count)
    {
        List<Integer> r = new ArrayList<>();
        for (int i = 0 ; i < count ; i++)
            r.add(i + 8);
        return r;
    }

    private static final Comparator<Integer> CMP = new Comparator<Integer>()
    {
        public int compare(Integer o1, Integer o2)
        {
            return Integer.compare(o1, o2);
        }
    };

    @Test
    public void testFirstHaha()
    {
        Object[] btree = BTree.build(seq(21), UpdateFunction.noOp());
        BTreeSearchIterator iter = new FullBTreeSearchIterator<>(btree, CMP, Dir.DESC);
        assertTrue(iter.hasNext());
        Integer key = 16;
        Integer val = (Integer) iter.next(key);
        BTreeSearchIterator iter2 = new LeafBTreeSearchIterator<>(btree, CMP, Dir.DESC);
        assertTrue(iter.hasNext());
        Integer val3 = (Integer) iter2.next(key);
        assertEquals(iter.indexOfCurrent(), iter2.indexOfCurrent());
        assertEquals(val, val3);
        assertEquals(1, 1);
    }
}
