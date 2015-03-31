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

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockitoAnnotations;

import org.apache.storm.topology.builder.Loader.LoadFailure;
import org.apache.utils.DefaultingMap;

import java.util.ArrayList;
import java.util.HashMap;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class BoltLoaderTest {

    protected final static String boltName = "tstBolt";

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks(this);
    }

    protected static String testObjectPath = "org.apache.storm.topology.builder.MockLoadableObject";

    @Test
    public void testLoad() throws LoadFailure {
        DefaultingMap aMap = new DefaultingMap();
        aMap.put("class", testObjectPath);
        aMap.put("val", "testval");
        aMap.put("parallelism", 2);

        ArrayList<Object> inputs = new ArrayList<Object>(3);
        inputs.add("srca:strma");
        inputs.add("srcb:strmb:all");
        inputs.add("srcc:strmc:fields:f1,f2");
        inputs.add("srcd");
        inputs.add("disabled");
        inputs.add("undefined");
        inputs.add("srcf::all");
        //shuffle, all, localOrShuffle, direct, none, or global
        inputs.add("losc:loss:localOrShuffle");
        inputs.add("dosc:doss:direct");
        inputs.add("nosc:noss:none");
        inputs.add("gosc:goss:global");

        HashMap<String,Object> customInput = new HashMap<String,Object>();
        customInput.put("class", testObjectPath);
        customInput.put("component", "srca");
        customInput.put("streamid", "strma");
        inputs.add(customInput);

        aMap.put("inputs", inputs);

        DefaultingMap bolts = new DefaultingMap();
        bolts.put(boltName, aMap);

        DefaultingMap conf = new DefaultingMap();
        conf.put("bolts", bolts);

        DefaultingMap spt = new DefaultingMap();
        spt.put("class", testObjectPath);
        DefaultingMap spouts = new DefaultingMap();
        spouts.put("srca", spt);
        spouts.put("srcb", spt);
        spouts.put("srcc", spt);
        spouts.put("srcd", spt);
        spouts.put("srcf", spt);
        spouts.put("losc", spt);
        spouts.put("nosc", spt);
        spouts.put("dosc", spt);
        spouts.put("gosc", spt);
        spt = new DefaultingMap();
        spt.put("class", testObjectPath);
        spt.put("parallelism", 0);
        spouts.put("disabled", spt);
        conf.put("spouts", spouts);

        TopologyBuilder bldrMock = mock(TopologyBuilder.class);
        BoltDeclarer declarerMock = mock(BoltDeclarer.class);

        when(bldrMock.setBolt(eq(boltName), (IRichBolt)anyObject(), eq(2))).thenReturn(declarerMock);

        TestConfigurator cfg = new TestConfigurator("testName");
        Loader ldr = new Loader(conf, bldrMock, cfg);
        Assert.assertNotNull(ldr);

        // Verify that the bolt was created from the config and added to topology.
        ArgumentCaptor<IRichBolt> setBoltCaptor = ArgumentCaptor.forClass(IRichBolt.class);
        verify(bldrMock,times(1)).setBolt(eq(boltName), setBoltCaptor.capture(), eq(2));
        IRichBolt irBolt = setBoltCaptor.getValue();

        // Verify that the bolt is the expected type, and was built with intended values
        Assert.assertNotNull(irBolt);
        Assert.assertTrue(irBolt instanceof MockLoadableObject);
        MockLoadableObject tlo = (MockLoadableObject)irBolt;
        Assert.assertEquals("testval", tlo.myVar());
        Assert.assertEquals(boltName, tlo.myName());

        // Now verify that inputs were set correctly.

        verify(declarerMock,times(1)).shuffleGrouping("srca", "strma");
        verify(declarerMock,times(1)).shuffleGrouping("srcd", Utils.DEFAULT_STREAM_ID);
        verify(declarerMock,times(0)).shuffleGrouping("disabled", Utils.DEFAULT_STREAM_ID);
        verify(declarerMock,times(0)).shuffleGrouping("undefined", Utils.DEFAULT_STREAM_ID);
        verify(declarerMock,times(1)).allGrouping("srcb", "strmb");
        verify(declarerMock,times(1)).allGrouping("srcf", Utils.DEFAULT_STREAM_ID);
        verify(declarerMock,times(1)).localOrShuffleGrouping("losc", "loss");
        verify(declarerMock,times(1)).directGrouping("dosc", "doss");
        verify(declarerMock,times(1)).noneGrouping("nosc", "noss");
        verify(declarerMock,times(1)).globalGrouping("gosc", "goss");

        ArgumentCaptor<CustomStreamGrouping> cgCaptor = ArgumentCaptor.forClass(CustomStreamGrouping.class);
        verify(declarerMock,times(1)).customGrouping(eq("srca"), eq("strma"), cgCaptor.capture());
        Assert.assertTrue("Unexpected customGrouping type", cgCaptor.getValue() instanceof MockLoadableObject);
        ArgumentCaptor<Fields> fCaptor = ArgumentCaptor.forClass(Fields.class);
        verify(declarerMock,times(1)).fieldsGrouping(eq("srcc"), eq("strmc"), fCaptor.capture());
        Fields oFields = fCaptor.getValue();
        Assert.assertEquals("f1", oFields.get(0));
        Assert.assertEquals("f2", oFields.get(1));
    }

    @Test(expected=IllegalArgumentException.class)
    public void testBadInput() throws LoadFailure {
        DefaultingMap aMap = new DefaultingMap();
        aMap.put("class", testObjectPath);
        aMap.put("val", "testval");
        aMap.put("parallelism", 2);

        ArrayList<Object> inputs = new ArrayList<Object>(3);
        inputs.add("srca::badgrp");

        aMap.put("inputs", inputs);

        TopologyBuilder bldrMock = mock(TopologyBuilder.class);
        BoltDeclarer declarerMock = mock(BoltDeclarer.class);

        when(bldrMock.setBolt(eq(boltName), (IRichBolt)anyObject(), eq(2))).thenReturn(declarerMock);

        TestConfigurator cfg = new TestConfigurator("testName");

        DefaultingMap bolts = new DefaultingMap();
        bolts.put(boltName, aMap);
        DefaultingMap conf = new DefaultingMap();
        conf.put("bolts", bolts);
        Loader ldr = new Loader(conf, bldrMock, cfg);
        Assert.assertNull(ldr);
    }
}
