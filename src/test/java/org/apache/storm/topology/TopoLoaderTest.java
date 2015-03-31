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
package org.apache.storm.topology;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import org.junit.Assert;
import org.junit.Test;

import org.apache.storm.topology.builder.Loader;
import org.apache.storm.topology.builder.Loader.LoadFailure;
import org.apache.storm.topology.builder.TestConfigurator;
import org.apache.utils.DefaultingMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TopoLoaderTest {
    protected static final String testYamlFile = "./src/test/resources/topologies/testtopology.yaml";
    protected static final String testOverride = "./src/test/resources/topologies/testoverride.yaml";


    protected Map<String,Object> makeMap() {
        HashMap<String, Object> hm = new HashMap<String,Object>();

        hm.put("true", new Boolean(true));
        hm.put("string", "astring");
        hm.put("int", new Integer(123));
        hm.put("long", new Long(567));
        hm.put("snum", "765");
        hm.put("intzero", new Integer(0));
        hm.put("intstr", "0");
        hm.put("strtrue", "true");

        HashMap<String,Object> subMap = new HashMap<String,Object>();
        subMap.put("smstr", "subString");
        hm.put("submap", subMap);

        return hm; 
    }

    @SuppressWarnings("unchecked")
    protected Map<String,Object> getSubMap(Map<String,Object> m, String key) {
        Object v = m.get(key);
        if ((v != null) && (v instanceof Map<?,?>)) {
            return (Map<String,Object>) v;
        }
        return null; 
    }

    @Test
    public void testCustomConfig() throws LoadFailure {

        DefaultingMap init1 = new DefaultingMap();
        init1.put("class", "org.apache.storm.topology.builder.MockLoadableObject");

        DefaultingMap init2 = new DefaultingMap(init1);
        init1.put("name", "i1");
        init2.put("name", "i2");
        init1.put("tv1", "x");
        init2.put("tv2", "y");

        // List<DefaultingMap> ml = new ArrayList<DefaultingMap>();
        HashMap<String,Object> ml = new HashMap<String,Object>();
        ml.put("cfg1", init1);
        ml.put("cfg2", init2);

        DefaultingMap yaml = new DefaultingMap();
        yaml.put("config", ml);

        TestConfigurator cfg = new TestConfigurator();

        Loader.doCustomConfig(cfg, yaml);
        Config stormConf = cfg.getStormConfig();

        Assert.assertEquals("x", stormConf.get("tv1"));
        Assert.assertEquals("y", stormConf.get("tv2"));

    }

    
    @Test
    public void testFlatten() {
        ArrayList<String> l1 = new ArrayList<String>();
        l1.add("aaa");
        l1.add("bbb");
        l1.add("ccc");
        
        ArrayList<String> l2 = new ArrayList<String>();
        l2.add("bbb");
        l2.add("zzz");
        
        ArrayList<Object> lt = new ArrayList<Object>();
        lt.add(l1);
        lt.add(l2);
        DefaultingMap conf = new DefaultingMap();
        conf.put("schema", lt);
        
        Set<String> schema = Loader.getSchema(conf);
        Assert.assertEquals(4,  schema.size());
        Assert.assertTrue(schema.contains("aaa"));
        Assert.assertTrue(schema.contains("bbb"));
        Assert.assertTrue(schema.contains("ccc"));
        Assert.assertTrue(schema.contains("zzz"));
        Assert.assertFalse(schema.contains("aaaa"));
    }

    @Test
    public void testMainTopo() throws IOException, LoadFailure {

        DefaultingMap conf = new DefaultingMap(TopoLoader.readYaml(testYamlFile));
        
        TestConfigurator cfg = new TestConfigurator();
        Loader ldr = new Loader(conf, cfg);
        
        List<String> spreads = ldr.getSpreads();
        Assert.assertNotNull(spreads);
        Assert.assertEquals(1, spreads.size());
        Assert.assertTrue(spreads.contains("Spout"));
        
        StormTopology topo = ldr.getTopology();
        Assert.assertNotNull(topo);
        
        // Do any topology-specific configuration.
        Loader.doCustomConfig(cfg, conf);
    }

    @Test
    public void testMerge() {
        Map<String,Object> hm = makeMap();
        Map<String,Object> oride = new HashMap<String,Object>();

        HashMap<String,Object> newSub = new HashMap<String,Object>();
        newSub.put("subKey", "subVal");
        oride.put("string", null);
        oride.put("newSub", newSub);

        HashMap<String,Object> updSub = new HashMap<String,Object>();
        updSub.put("newsubkey", "ns");
        oride.put("submap", updSub);

        Assert.assertNull(hm.get("newSub"));
        Assert.assertNotNull(hm.get("string"));
        Map<String,Object> nsm = getSubMap(hm, "submap");
        Assert.assertNull(nsm.get("newsubkey"));
        Assert.assertEquals("subString",nsm.get("smstr"));

        TopoLoader.mergeYaml(hm, oride);

        Assert.assertEquals("subVal", getSubMap(hm, "newSub").get("subKey"));
        Assert.assertNull(hm.get("string"));
        nsm = getSubMap(hm, "submap");
        Assert.assertEquals("ns", nsm.get("newsubkey"));
        Assert.assertEquals("subString", nsm.get("smstr"));
    }

    @Test
    public void testReplace() {
        Map<String,Object> hm = makeMap();
        Map<String,Object> oride = new HashMap<String,Object>();

        HashMap<String,Object> newSub = new HashMap<String,Object>();
        newSub.put("subKey", "subVal");
        oride.put("string", null);
        oride.put("newSub", newSub);

        HashMap<String,Object> updSub = new HashMap<String,Object>();
        updSub.put("newsubkey", "ns");
        updSub.put("replaceSection", true); 
        oride.put("submap", updSub);

        Assert.assertNull(getSubMap(hm, "newSub"));
        Assert.assertNotNull(hm.get("string"));
        Assert.assertNull(getSubMap(hm, "submap").get("newsubkey"));

        TopoLoader.mergeYaml(hm, oride);

        Assert.assertEquals("subVal", getSubMap(hm, "newSub").get("subKey"));
        Assert.assertNull(hm.get("string"));
        Map<String,Object> nsm = getSubMap(hm, "submap");
        Assert.assertEquals("ns", nsm.get("newsubkey"));
        Assert.assertNull(nsm.get("smstr"));
    }
    
    @Test
    public void testPatchSchema() {
        DefaultingMap yaml = new DefaultingMap();
        
        ArrayList<String> theSchema = new ArrayList<String>();
        theSchema.add("keep");
        theSchema.add("remove");
        
        DefaultingMap theBolt = new DefaultingMap();
        theBolt.put("schema", theSchema);
        
        DefaultingMap bolts = new DefaultingMap();
        bolts.put("testBolt", theBolt);
        
        yaml.put("bolts", bolts);

        DefaultingMap aPatch = new DefaultingMap();
        ArrayList<String> adds = new ArrayList<String>();
        adds.add("add");
        aPatch.put("add", adds);
        
        ArrayList<String> removes = new ArrayList<String>();
        removes.add("remove");
        aPatch.put("remove", removes);
        
        DefaultingMap patches = new DefaultingMap();
        patches.put("bolts>testBolt>schema", aPatch);
        yaml.put("listpatch", patches);
        
        Assert.assertTrue(TopoLoader.patchSchemas(yaml));
        
        Set<String> newSchema = theBolt.getFlattenedList("schema");
        Assert.assertEquals(2, newSchema.size());
        Assert.assertTrue(newSchema.contains("add"));
        Assert.assertTrue(newSchema.contains("keep"));
        Assert.assertFalse(newSchema.contains("remove"));
        
        // Test if schema doesn't exist
        theBolt.remove("schema");
        Assert.assertTrue(TopoLoader.patchSchemas(yaml));
        newSchema = theBolt.getFlattenedList("schema");
        Assert.assertEquals(1, newSchema.size());
        Assert.assertTrue(newSchema.contains("add"));
        Assert.assertFalse(newSchema.contains("keep"));
        Assert.assertFalse(newSchema.contains("remove"));
        
        // Test if bolt doesn't exist
        bolts.remove("testBolt");
        Assert.assertFalse(TopoLoader.patchSchemas(yaml));
    }

    @Test
    public void testFileLoad() {
        Map<String, Object> theYaml = null;

        theYaml = TopoLoader.readYaml(testYamlFile);
        Assert.assertNotNull(theYaml);
        Assert.assertTrue("processOverrides failed", TopoLoader.processOverrides(testOverride, theYaml));
        Assert.assertTrue("patchSchemas failed", TopoLoader.patchSchemas(theYaml));

        @SuppressWarnings("unchecked")
        List<String> schema = (List<String>)((Map<String,Object>)((Map<String,Object>)theYaml.
                get("spouts")).
                get("Spout")).get("schema");
        Assert.assertTrue("listpatch failed", schema.contains("event_uuid"));
    }

    @Test
    public void testSubmit() throws InterruptedException, LoadFailure {
        String args[] = {"--dryrun", "-n", "TestTopo", "--overrides", testOverride, testYamlFile};

        TopoLoader tl = new TopoLoader();
        Assert.assertEquals (0, tl.runLoader(args));
    }

}
