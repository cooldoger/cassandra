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

package org.apache.cassandra.service;

import java.net.InetAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.VersionedValue;

import static org.junit.Assert.assertEquals;

public class StorageServiceStateChangeTest
{

    @Test
    public void testRemoveEndpointsNoLonggerOwningTokens() throws Exception
    {
        DatabaseDescriptor.setDaemonInitialized();
        VersionedValue.VersionedValueFactory valueFactory =
            new VersionedValue.VersionedValueFactory(DatabaseDescriptor.getPartitioner());

        Collection<Token> tokens = new HashSet<>();
        tokens.add(DatabaseDescriptor.getPartitioner().getRandomToken());
        tokens.add(DatabaseDescriptor.getPartitioner().getRandomToken());

        // Initialize a node with 2 tokens
        InetAddress ep1 = InetAddress.getByName("127.0.0.1");
        Gossiper.instance.initializeNodeUnsafe(ep1, UUID.randomUUID(), 1);
        Gossiper.instance.injectApplicationState(ep1, ApplicationState.TOKENS, valueFactory.tokens(tokens));
        StorageService.instance.onChange(ep1, ApplicationState.STATUS, valueFactory.normal(tokens));

        final AtomicInteger removedNum = new AtomicInteger(0);
        Gossiper.instance.register(
        new IEndpointStateChangeSubscriber()
        {
            public void onJoin(InetAddress endpoint, EndpointState epState) { }

            public void beforeChange(InetAddress endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue) { }

            public void onChange(InetAddress endpoint, ApplicationState state, VersionedValue value) { }

            public void onAlive(InetAddress endpoint, EndpointState state) { }

            public void onDead(InetAddress endpoint, EndpointState state) { }

            public void onRemove(InetAddress endpoint)
            {
                removedNum.incrementAndGet();
                assertEquals(ep1, endpoint);
            }

            public void onRestart(InetAddress endpoint, EndpointState state) { }
        }
        );

        // Adding a second node ep2 with the same tokens, make sure ep1 is removed, as it's no longer owning any tokens.
        assertEquals(0, removedNum.get());
        InetAddress ep2 = InetAddress.getByName("127.0.0.2");
        Gossiper.instance.initializeNodeUnsafe(ep2, UUID.randomUUID(), 2);
        Gossiper.instance.injectApplicationState(ep2, ApplicationState.TOKENS, valueFactory.tokens(tokens));
        StorageService.instance.onChange(ep2, ApplicationState.STATUS, valueFactory.normal(tokens));
        assertEquals(1, removedNum.get());
    }
}
