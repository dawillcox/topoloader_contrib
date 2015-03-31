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

import org.junit.Assert;
import org.junit.Test;

import org.apache.storm.topology.builder.Loader.LoadFailure;
import org.apache.utils.DefaultingMap;

public class LoaderTest {


    @Test
    public void testBuild() throws LoadFailure {
        
        DefaultingMap aMap = new DefaultingMap();
        aMap.put("class",  "org.apache.storm.topology.builder.MockLoadableObject");
        aMap.put("val", "testval");
        
        Object o = Loader.loadAndBuild("Test", aMap);
        
        Assert.assertNotNull(o);
        Assert.assertTrue(o instanceof MockLoadableObject);
        
        MockLoadableObject bt = (MockLoadableObject)o;
        Assert.assertEquals("testval", bt.myVar());
        Assert.assertEquals("Test", bt.myName());
        
    }
    
    @Test(expected=IllegalArgumentException.class)
    public void testIllegal() throws LoadFailure {
        DefaultingMap aMap = new DefaultingMap();
        
        TestConfigurator cfg = new TestConfigurator();
        @SuppressWarnings("unused")
        Loader ldr = new Loader(aMap, cfg);
    }

    @Test
    public void testBuilder() throws LoadFailure {
        DefaultingMap conf = new DefaultingMap();
        conf.put("builder", "org.apache.storm.topology.builder.LoaderTest:testBuilder");
        Object o = Loader.loadAndBuild("tst", conf);
        Assert.assertTrue("Not expected type", o instanceof MockLoadableObject);
    }

    @Test
    public void testBuildDflt() throws LoadFailure {
        DefaultingMap conf = new DefaultingMap();
        conf.put("builder", "org.apache.storm.topology.builder.LoaderTest");
        Object o = Loader.loadAndBuild("tst", conf);
        Assert.assertEquals("tst", o);
    }

    @Test(expected=LoadFailure.class)
    public void testBadBuilder() throws LoadFailure {
        DefaultingMap conf = new DefaultingMap();
        conf.put("builder", "org.apache.storm.topology.builder.LoaderTestNot");
        Object o = Loader.loadAndBuild("tst", conf);
        Assert.assertEquals("tst", o);
    }

    public static Object testBuilder(String name, DefaultingMap conf) {
        return new MockLoadableObject(name, conf);
    }

    public static Object builder(String name, DefaultingMap conf) {
        return name;
    }

}
