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
    static
    {
        System.setProperty("cassandra.btree.fanfactor", "32");
    }

    private static List<Integer> seq(int count)
    {
        List<Integer> r = new ArrayList<>();
        for (int i = 0 ; i < count ; i++)
            r.add(i);
        return r;
    }

    private static final Comparator<Integer> CMP = new Comparator<Integer>()
    {
        public int compare(Integer o1, Integer o2)
        {
            return Integer.compare(o1, o2);
        }
    };

    private static void assertIteratorsEqual(final String msg, final Iterator<Integer> iter1, final Iterator<Integer> iter2)
    {
        int iterCount = 0;
        while (iter1.hasNext())
        {
            final String message = msg + " on iteration " + iterCount;
            ++iterCount;
            assertTrue(message, iter2.hasNext());
            assertEquals(message, iter1.next(), iter2.next());
        }
        assertFalse(msg, iter2.hasNext());
    }

    private static void assertBTreeSearchIteratorEquals(final String msg,
                                                        final BTreeSearchIterator<Integer, Integer> iter1,
                                                        final BTreeSearchIterator<Integer, Integer> iter2)
    {
        while(iter1.hasNext())
        {
            assertTrue(msg, iter2.hasNext());
            assertEquals(msg, iter1.next(), iter2.next(iter1.current()));
            assertEquals(msg, iter1.current(), iter2.current());
            assertEquals(msg, iter1.indexOfCurrent(), iter2.indexOfCurrent());
        }
        assertFalse(msg, iter2.hasNext());
    }

    private static void assertIndexedSearchIterators(final String msg,
                                                     final IndexedSearchIterator<Integer, Integer>iter1,
                                                     final IndexedSearchIterator<Integer, Integer> iter2,
                                                     final Integer key1,
                                                     final Integer key2)
    {
        assertEquals(msg, iter1.next(key1), iter2.next(key1));
        // called twice to make sure that it does not move the iterator
        assertEquals(msg, iter1.current(), iter2.current());
        assertEquals(msg, iter1.current(), iter2.current());
        // called twice to make sure that it does not move the iterator
        assertEquals(msg, iter1.indexOfCurrent(), iter2.indexOfCurrent());
        assertEquals(msg, iter1.indexOfCurrent(), iter2.indexOfCurrent());
        assertEquals(msg, iter1.next(key2), iter2.next(key2));
        // called twice to make sure that it does not move the iterator
        assertEquals(msg, iter1.current(), iter2.current());
        assertEquals(msg, iter1.current(), iter2.current());
        // called twice to make sure that it does not move the iterator
        assertEquals(msg, iter1.indexOfCurrent(), iter2.indexOfCurrent());
        assertEquals(msg, iter1.indexOfCurrent(), iter2.indexOfCurrent());
    }

    @Test
    public void testLeafTree_empty()
    {
        assertFalse(LeafBTreeSearchIterators.iterator(BTree.empty(), null, Dir.ASC).hasNext());
        assertFalse(LeafBTreeSearchIterators.iterator(BTree.empty(), null, Dir.DESC).hasNext());
    }

    @Test
    public void testLeafTree_singleElement()
    {
        final Object[] btree = BTree.build(seq(1), UpdateFunction.noOp());
        assertTrue(LeafBTreeSearchIterators.iterator(btree, null, Dir.ASC).hasNext());
        assertTrue(LeafBTreeSearchIterators.iterator(btree, null, Dir.DESC).hasNext());
        {
            final Iterator<Integer> iter = LeafBTreeSearchIterators.iterator(btree, null, Dir.ASC);
            assertTrue(iter.hasNext());
            assertEquals(Integer.valueOf(0), iter.next());
            assertFalse(iter.hasNext());
        }
        {
            final Iterator<Integer> iter = LeafBTreeSearchIterators.iterator(btree, null, Dir.DESC);
            assertTrue(iter.hasNext());
            assertEquals(Integer.valueOf(0), iter.next());
            assertFalse(iter.hasNext());
        }
        {
            final IndexedSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, Dir.ASC);
            assertStep(iter, 0, 0);
            assertFinished(iter, 0);
        }
        {
            final IndexedSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, Dir.DESC);
            assertStep(iter, 0, 0);
            assertFinished(iter, 0);
        }
    }

    private void assertStep(final IndexedSearchIterator<Integer, Integer> iter, final int index, final int value)
    {
        assertTrue(iter.hasNext());
        assertEquals(Integer.valueOf(value), iter.next(value));
        assertEquals(Integer.valueOf(value), iter.current());
        assertEquals(index, iter.indexOfCurrent());
    }

    private void assertFinished(final IndexedSearchIterator<Integer, Integer> iter, final int value)
    {
        assertFalse(iter.hasNext());
        assertNull(iter.next(value));
        try {
            iter.current();
            fail();
        } catch (NoSuchElementException e) {
            // expected
        }
        try {
            iter.indexOfCurrent();
            fail();
        } catch (NoSuchElementException e) {
            // expected
        }
    }

    @Test
    public void testLeafTree_twoElements()
    {
        final Object[] btree = BTree.build(seq(2), UpdateFunction.noOp());
        assertTrue(LeafBTreeSearchIterators.iterator(btree, null, Dir.ASC).hasNext());
        assertTrue(LeafBTreeSearchIterators.iterator(btree, null, Dir.DESC).hasNext());
        {
            final Iterator<Integer> iter = LeafBTreeSearchIterators.iterator(btree, null, Dir.ASC);
            assertTrue(iter.hasNext());
            assertEquals(Integer.valueOf(0), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(Integer.valueOf(1), iter.next());
            assertFalse(iter.hasNext());
        }
        {
            final Iterator<Integer> iter = LeafBTreeSearchIterators.iterator(btree, null, Dir.DESC);
            assertTrue(iter.hasNext());
            assertEquals(Integer.valueOf(1), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(Integer.valueOf(0), iter.next());
            assertFalse(iter.hasNext());
        }
        {
            final IndexedSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, Dir.ASC);
            assertStep(iter, 0, 0);
            assertStep(iter, 1, 1);
            assertFinished(iter, 1);
        }
        {
            final IndexedSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, Dir.DESC);
            assertStep(iter, 0, 1);
            assertStep(iter, 1, 0);
            assertFinished(iter, 0);
        }
        {
            final IndexedSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, Dir.ASC);
            assertStep(iter, 1, 1);
            assertFalse(iter.hasNext());
            assertFinished(iter, 1);
        }
        {
            final IndexedSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, Dir.DESC);
            assertStep(iter, 1, 0);
            assertFinished(iter, 0);
        }
    }

    @Test
    public void testLeafTree_twoElementsWithAGap()
    {
        final Object[] btree = BTree.build(Arrays.asList(0, 2), UpdateFunction.noOp());
        assertTrue(LeafBTreeSearchIterators.iterator(btree, null, Dir.ASC).hasNext());
        assertTrue(LeafBTreeSearchIterators.iterator(btree, null, Dir.DESC).hasNext());
        {
            final Iterator<Integer> iter = LeafBTreeSearchIterators.iterator(btree, null, Dir.ASC);
            assertTrue(iter.hasNext());
            assertEquals(Integer.valueOf(0), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(Integer.valueOf(2), iter.next());
            assertFalse(iter.hasNext());
        }
        {
            final Iterator<Integer> iter = LeafBTreeSearchIterators.iterator(btree, null, Dir.DESC);
            assertTrue(iter.hasNext());
            assertEquals(Integer.valueOf(2), iter.next());
            assertTrue(iter.hasNext());
            assertEquals(Integer.valueOf(0), iter.next());
            assertFalse(iter.hasNext());
        }
        {
            final IndexedSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, Dir.ASC);
            assertStep(iter, 0, 0);
            assertTrue(iter.hasNext());
            assertNull(iter.next(1));
            try {
                iter.current();
                fail();
            } catch (NoSuchElementException e) {
                // expected
            }
            try {
                iter.indexOfCurrent();
                fail();
            } catch (NoSuchElementException e) {
                // expected
            }
            assertStep(iter, 1, 2);
            assertFinished(iter, 2);
        }
        {
            final IndexedSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, Dir.DESC);
            assertStep(iter, 0, 2);
            assertTrue(iter.hasNext());
            assertNull(iter.next(1));
            try {
                iter.current();
                fail();
            } catch (NoSuchElementException e) {
                // expected
            }
            try {
                iter.indexOfCurrent();
                fail();
            } catch (NoSuchElementException e) {
                // expected
            }
            assertStep(iter, 1, 0);
            assertFinished(iter, 0);
        }
        {
            final IndexedSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, Dir.ASC);
            assertStep(iter, 1, 2);
            assertFalse(iter.hasNext());
            assertFinished(iter, 1);
        }
        {
            final IndexedSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, Dir.DESC);
            assertStep(iter, 1, 0);
            assertFinished(iter, 0);
        }
    }

    @Test
    public void testLeafTree_compareLeafAndFullIterators()
    {
        for (int size = 1; size < 32; ++size)
        {
            final Object[] btree = BTree.build(seq(size), UpdateFunction.noOp());
            final String msg = "with size " + size;

            {
                final Iterator<Integer> leaf = LeafBTreeSearchIterators.iterator  (btree, null, Dir.ASC);
                final FullBTreeSearchIterator<Integer, Integer> full =
                        new FullBTreeSearchIterator<>(btree, null, Dir.ASC);
                assertIteratorsEqual(msg, full, leaf);
            }

            {
                final Iterator<Integer> leaf = LeafBTreeSearchIterators.iterator(btree, null, Dir.DESC);
                final FullBTreeSearchIterator<Integer, Integer> full =
                        new FullBTreeSearchIterator<>(btree, null, Dir.DESC);
                assertIteratorsEqual(msg, full, leaf);
            }

            for (int lowerBound = 0; lowerBound < size; ++lowerBound)
                for (int upperBound = lowerBound; upperBound < size; ++upperBound)
                {
                    final String message = msg + " with lower bound " + lowerBound + " and upper bound " + upperBound;
                    {
                        final Iterator<Integer> leaf =
                                LeafBTreeSearchIterators.iterator(btree, null, Dir.ASC, lowerBound, upperBound);
                        final FullBTreeSearchIterator<Integer, Integer> full =
                                new FullBTreeSearchIterator<>(btree, null, Dir.ASC, lowerBound, upperBound);
                        assertIteratorsEqual(message, full, leaf);
                    }

                    {
                        final Iterator<Integer> leaf =
                                LeafBTreeSearchIterators.iterator(btree, null, Dir.DESC, lowerBound, upperBound);
                        final FullBTreeSearchIterator<Integer, Integer> full =
                                new FullBTreeSearchIterator<>(btree, null, Dir.DESC, lowerBound, upperBound);
                        assertIteratorsEqual(message, full, leaf);
                    }

                    {
                        final BTreeSearchIterator<Integer, Integer> leaf =
                                LeafBTreeSearchIterators.iterator(btree, CMP, Dir.ASC, lowerBound, upperBound);
                        final FullBTreeSearchIterator<Integer, Integer> full =
                                new FullBTreeSearchIterator<>(btree, CMP, Dir.ASC, lowerBound, upperBound);
                        assertBTreeSearchIteratorEquals(message, full, leaf);
                    }

                    {
                        final BTreeSearchIterator<Integer, Integer> leaf =
                                LeafBTreeSearchIterators.iterator(btree, CMP, Dir.DESC, lowerBound, upperBound);
                        final FullBTreeSearchIterator<Integer, Integer> full =
                                new FullBTreeSearchIterator<>(btree, CMP, Dir.DESC, lowerBound, upperBound);
                        assertBTreeSearchIteratorEquals(message, full, leaf);
                    }

                    {
                        final BTreeSearchIterator<Integer, Integer> leaf =
                                LeafBTreeSearchIterators.iterator(btree, CMP, Dir.ASC, lowerBound, upperBound);
                        final FullBTreeSearchIterator<Integer, Integer> full =
                                new FullBTreeSearchIterator<>(btree, CMP, Dir.ASC, lowerBound, upperBound);
                        assertBTreeSearchIteratorEquals(message, leaf, full);
                    }

                    {
                        final BTreeSearchIterator<Integer, Integer> leaf =
                                LeafBTreeSearchIterators.iterator(btree, CMP, Dir.DESC, lowerBound, upperBound);
                        final FullBTreeSearchIterator<Integer, Integer> full =
                                new FullBTreeSearchIterator<>(btree, CMP, Dir.DESC, lowerBound, upperBound);
                        assertBTreeSearchIteratorEquals(message, leaf, full);
                    }

                    for (int i = lowerBound; i < upperBound; ++i)
                        for (int j = i + 1; j <= upperBound; ++j)
                        {
                            {
                                final BTreeSearchIterator<Integer, Integer> leaf =
                                        LeafBTreeSearchIterators.iterator(btree, CMP, Dir.ASC, lowerBound, upperBound);
                                final FullBTreeSearchIterator<Integer, Integer> full =
                                        new FullBTreeSearchIterator<>(btree, CMP, Dir.ASC, lowerBound, upperBound);
                                assertIndexedSearchIterators(message + " seek " + i + " and " + j, full, leaf, i, j);
                            }

                            {
                                final BTreeSearchIterator<Integer, Integer> leaf =
                                        LeafBTreeSearchIterators.iterator(btree, CMP, Dir.DESC, lowerBound, upperBound);
                                final FullBTreeSearchIterator<Integer, Integer> full =
                                        new FullBTreeSearchIterator<>(btree, CMP, Dir.DESC, lowerBound, upperBound);
                                assertIndexedSearchIterators(message + " seek " + j + " and " + i, full, leaf, j, i);
                            }
                        }

                }
        }
    }

    /**
     * Generates performance comparison between FullBTreeSearchIterator and LeafBTreeSearchIterator.
     */
    public static void main(String[] args) {
        final int repCount = 1000000;
        // this variable is here to ensure that compiler does not optimize out the loop
        int sum = 0;
        sum += comparePerformance(repCount, Dir.ASC);
        sum += comparePerformance(repCount, Dir.DESC);
        sum = sum + 1;
    }

    private static long comparePerformance(int repCount, Dir dir)
    {
        // this variable is here to ensure that compiler does not optimize out the loop
        int sum = 0;
        System.out.println("Iteration over all elements " + repCount + " times in direction " + dir + ":");
        for (int size = 1; size < 32; ++size)
        {
            final Object[] btree = BTree.build(seq(size), UpdateFunction.noOp());
            final long fullStart = System.currentTimeMillis();
            for (int rep = 0; rep < repCount; ++rep)
            {
                final BTreeSearchIterator<Integer, Integer> iter = new FullBTreeSearchIterator<>(btree, CMP, dir);
                while (iter.hasNext())
                    sum += iter.next();
            }
            final long fullEnd = System.currentTimeMillis();
            sum = 0;
            final long leafStart = System.currentTimeMillis();
            for (int rep = 0; rep < repCount; ++rep)
            {
                final BTreeSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, dir);
                while (iter.hasNext())
                    sum += iter.next();
            }
            final long leafEnd = System.currentTimeMillis();
            System.out.println("\tFor size " + size +
                    " full took " + (fullEnd - fullStart) + " ms and leaf took " + (leafEnd - leafStart) + " ms");
        }
        System.out.println("Iteration over all elements with next(key) " + repCount + " times in direction " + dir + ":");
        for (int size = 1; size < 32; ++size)
        {
            final Object[] btree = BTree.build(seq(size), UpdateFunction.noOp());
            int step = dir == Dir.ASC ? 1 : -1;
            int start = dir == Dir.ASC ? 0 : size - 1;
            final long fullStart = System.currentTimeMillis();
            for (int rep = 0; rep < repCount; ++rep)
            {
                final BTreeSearchIterator<Integer, Integer> iter = new FullBTreeSearchIterator<>(btree, CMP, dir);
                int next = start;
                while (iter.hasNext())
                {
                    sum += iter.next(next);
                    next += step;
                }
            }
            final long fullEnd = System.currentTimeMillis();
            sum = 0;
            final long leafStart = System.currentTimeMillis();
            for (int rep = 0; rep < repCount; ++rep)
            {
                final BTreeSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, dir);
                int next = start;
                while (iter.hasNext())
                {
                    sum += iter.next(next);
                    next += step;
                }
            }
            final long leafEnd = System.currentTimeMillis();
            System.out.println("\tFor size " + size +
                    " full took " + (fullEnd - fullStart) + " ms and leaf took " + (leafEnd - leafStart) + " ms");
        }
        System.out.println("Jump to last element with next(key) " + repCount + " times in direction " + dir + ":");
        for (int size = 1; size < 32; ++size)
        {
            final Object[] btree = BTree.build(seq(size), UpdateFunction.noOp());
            int lastElem = dir == Dir.ASC ? size - 1 : 0;
            final long fullStart = System.currentTimeMillis();
            for (int rep = 0; rep < repCount; ++rep)
            {
                final BTreeSearchIterator<Integer, Integer> iter = new FullBTreeSearchIterator<>(btree, CMP, dir);
                sum += iter.next(lastElem);
            }
            final long fullEnd = System.currentTimeMillis();
            sum = 0;
            final long leafStart = System.currentTimeMillis();
            for (int rep = 0; rep < repCount; ++rep)
            {
                final BTreeSearchIterator<Integer, Integer> iter = LeafBTreeSearchIterators.iterator(btree, CMP, dir);
                sum += iter.next(lastElem);
            }
            final long leafEnd = System.currentTimeMillis();
            System.out.println("\tFor size " + size +
                    " full took " + (fullEnd - fullStart) + " ms and leaf took " + (leafEnd - leafStart) + " ms");
        }
        return sum;
    }
}
