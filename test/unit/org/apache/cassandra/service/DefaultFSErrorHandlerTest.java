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

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.FSErrorHandler;
import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultFSErrorHandlerTest
{
    FSErrorHandler handler = new DefaultFSErrorHandler();
    Config.DiskFailurePolicy oldDiskPolicy;
    Config.CorruptSSTablePolicy oldCorruptPolicy;

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        SchemaLoader.prepareServer();
        CassandraDaemon daemon = new CassandraDaemon();
        daemon.completeSetup();
        StorageService.instance.registerDaemon(daemon);
        StorageService.instance.initServer();
    }

    @Before
    public void setup()
    {
        StorageService.instance.startGossiping();
        assertTrue(Gossiper.instance.isEnabled());
        oldDiskPolicy = DatabaseDescriptor.getDiskFailurePolicy();
        oldCorruptPolicy = DatabaseDescriptor.getCorruptSSTablePolicy();
    }

    @After
    public void teardown()
    {
        DatabaseDescriptor.setDiskFailurePolicy(oldDiskPolicy);
        DatabaseDescriptor.setCorruptSSTablePolicy(oldCorruptPolicy);
    }

    @Test
    public void testFSErrorPolicyIgnore()
    {
        assertEquals(DatabaseDescriptor.getDiskFailurePolicy(), Config.DiskFailurePolicy.ignore);
        handler.handleFSError(new FSReadError(new IOException(), "blah"));
        assertTrue(Gossiper.instance.isEnabled());
    }

    @Test
    public void testFSErrorPolicyStop()
    {
        DatabaseDescriptor.setDiskFailurePolicy(Config.DiskFailurePolicy.stop);
        handler.handleFSError(new FSReadError(new IOException(), "blah"));
        assertFalse(Gossiper.instance.isEnabled());
    }

    @Test
    public void testCorruptSSTablePolicyStop()
    {
        DatabaseDescriptor.setCorruptSSTablePolicy(Config.CorruptSSTablePolicy.stop);
        handler.handleCorruptSSTable(new CorruptSSTableException(new IOException(), "/file/tmp.db"));
        assertFalse(Gossiper.instance.isEnabled());
    }

    @Test
    public void testCorruptSSTablePolicyIgnore()
    {
        // Default policy is ignore
        assertEquals(DatabaseDescriptor.getCorruptSSTablePolicy(), Config.CorruptSSTablePolicy.ignore);
        handler.handleCorruptSSTable(new CorruptSSTableException(new IOException(), "/file/tmp.db"));
        assertTrue(Gossiper.instance.isEnabled());
    }

}
