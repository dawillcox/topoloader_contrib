/**
 * Copyright 2014, 2015, Yahoo, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.topology.builder;

import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;

import org.apache.storm.topology.builder.Loader.LoadFailure;
import org.apache.utils.DefaultingMap;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class SpoutLoaderTest {
    
    protected final static String spoutName = "tstSpout";
    
    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }
    
    
    @Test
    public void testLoad() throws LoadFailure {
        DefaultingMap aMap = new DefaultingMap();
        aMap.put("class", "org.apache.storm.topology.builder.MockLoadableObject");
        aMap.put("val", "testval");
        aMap.put("parallelism", 3);
        
        TopologyBuilder bldrMock = mock(TopologyBuilder.class);
        SpoutDeclarer declarerMock = mock(SpoutDeclarer.class);
        
        when(bldrMock.setSpout(eq(spoutName), (IRichSpout)anyObject(), eq(3))).thenReturn(declarerMock);

        TestConfigurator cfg = new TestConfigurator();
        DefaultingMap spouts = new DefaultingMap();
        spouts.put(spoutName, aMap);
        DefaultingMap conf = new DefaultingMap();
        conf.put("spouts", spouts);
        Loader ldr = new Loader(conf, bldrMock, cfg);
        Assert.assertNotNull(ldr);
        
        // Verify that the bolt was created from the config and added to topology.
        ArgumentCaptor<IRichSpout> setSpoutCaptor = ArgumentCaptor.forClass(IRichSpout.class);
        verify(bldrMock,times(1)).setSpout(eq(spoutName), setSpoutCaptor.capture(), eq(3));
        IRichSpout irSpout = setSpoutCaptor.getValue();
        
        // Verify that the bolt is the expected type, and was built with intended values
        Assert.assertNotNull(irSpout);
        Assert.assertTrue(irSpout instanceof MockLoadableObject);
        MockLoadableObject tlo = (MockLoadableObject)irSpout;
        Assert.assertEquals("testval", tlo.myVar());
        Assert.assertEquals(spoutName, tlo.myName());
        
    }

}
