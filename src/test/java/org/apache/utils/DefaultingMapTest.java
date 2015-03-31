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
package org.apache.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.junit.*;


public class DefaultingMapTest {

    public DefaultingMapTest() {
        // TODO Auto-generated constructor stub
    }

    protected DefaultingMap makeMap() {
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

        return new DefaultingMap(hm);
    }

    @Test
    public void testDefaults() {
        DefaultingMap hm = makeMap();
        Assert.assertTrue(hm.get("true", true));
        Assert.assertTrue(hm.get("true", false));
        Assert.assertFalse(hm.get("notthere", false));

        Assert.assertEquals(hm.get("int", 999), new Integer(123));
        Assert.assertEquals(hm.get("long", 999L), new Long(567));

        Assert.assertEquals(hm.get("notthere", 888), new Integer(888));
        Assert.assertEquals(hm.get("notthere", 999L), new Long(999));

        Assert.assertEquals(hm.get("string", "dflt"), "astring");
        Assert.assertEquals(hm.get("notthere", "dflt"), "dflt");

    }

    @Test(expected=ClassCastException.class)
    public void testBadType() {
        DefaultingMap hm = makeMap();

        Integer i = hm.get("long",  10);
        System.out.println(i);

    }

    @Test
    public void testContains()  {
        DefaultingMap hm = makeMap();
        Assert.assertTrue(hm.containsKey("int"));
        Assert.assertFalse(hm.containsKey("notthere"));
    }

    @Test
    public void testLong() {
        DefaultingMap hm = makeMap();
        Assert.assertEquals(123L, hm.getLong("int", 0L));
        Assert.assertEquals(567L, hm.getLong("long", 0L));
        Assert.assertEquals(765L, hm.getLong("snum", 0L));
        Assert.assertEquals(888L, hm.getLong("notthere", 888L));
        Assert.assertEquals(-1, hm.getLong("string", -1));
    }

    @Test
    public void testInteger() {
        DefaultingMap hm = makeMap();
        Assert.assertEquals(123, hm.getInt("int", 0));
        Assert.assertEquals(567, hm.getInt("long", 0));
        Assert.assertEquals(765, hm.getInt("snum", 0));
        Assert.assertEquals(888, hm.getInt("notthere", 888));
    }


    @Test
    public void testString() {
        DefaultingMap hm = makeMap();
        Assert.assertEquals("123", hm.getString("int", null));
        Assert.assertEquals("567", hm.getString("long", null));
        Assert.assertEquals("765", hm.getString("snum", null));
        Assert.assertEquals("xyz", hm.getString("notthere", "xyz"));
        Assert.assertNull(hm.getString("notthere", null));
        Assert.assertNull(hm.getString("notthere"));
        Assert.assertEquals("567", hm.getString("long"));
        Assert.assertNull(hm.get("notthere"));
        Assert.assertEquals("765", hm.get("snum").toString());
    }

    @Test
    public void testBool() {
        DefaultingMap hm = makeMap();
        Assert.assertTrue(hm.getBool("notthere", true));
        Assert.assertFalse(hm.getBool("notthere", false));
        Assert.assertTrue(hm.getBool("true", false));
        Assert.assertFalse(hm.getBool("intzero", true));
        Assert.assertTrue(hm.getBool("strtrue", false));
    }

    @Test
    public void getSubMap() {
        DefaultingMap hm = makeMap();
        DefaultingMap subMap = hm.getSubMap("submap");
        Assert.assertNotNull(subMap);
        Assert.assertEquals("subString", subMap.getString("smstr", "fail"));
    }

    @Test
    public void testGlobal() {
        DefaultingMap hm = makeMap();
        String gblkey = DefaultingMap.GLOBALPREFIX + "k";
        hm.put(gblkey, "gval");
        DefaultingMap subMap = hm.getSubMap("submap");
        Assert.assertNotNull(subMap);
        Assert.assertEquals("gval", subMap.getString(gblkey));
    }

    @Test
    public void testMerge() {
        DefaultingMap hm = makeMap();
        DefaultingMap oride = new DefaultingMap();

        HashMap<String,Object> newSub = new HashMap<String,Object>();
        newSub.put("subKey", "subVal");
        oride.put("string", null);
        oride.put("newSub", newSub);

        HashMap<String,Object> updSub = new HashMap<String,Object>();
        updSub.put("newsubkey", "ns");
        oride.put("submap", updSub);

        Assert.assertNull(hm.getSubMap("newSub"));
        Assert.assertNotNull(hm.getString("string"));
        DefaultingMap nsm = hm.getSubMap("submap");
        Assert.assertNull(nsm.getString("newsubkey"));
        Assert.assertEquals("subString",nsm.getString("smstr"));

        hm.mergeMap(oride);

        Assert.assertEquals("subVal", hm.getSubMap("newSub").getString("subKey"));
        Assert.assertNull(hm.getString("string"));
        nsm = hm.getSubMap("submap");
        Assert.assertEquals("ns", nsm.getString("newsubkey"));
        Assert.assertEquals("subString", nsm.getString("smstr"));
    }

    @Test
    public void testReplace() {
        DefaultingMap hm = makeMap();
        DefaultingMap oride = new DefaultingMap();

        HashMap<String,Object> newSub = new HashMap<String,Object>();
        newSub.put("subKey", "subVal");
        oride.put("string", null);
        oride.put("newSub", newSub);
        oride.put("replaceall", true);

        HashMap<String,Object> updSub = new HashMap<String,Object>();
        updSub.put("newsubkey", "ns");
        oride.put("submap", updSub);

        Assert.assertNull(hm.getSubMap("newSub"));
        Assert.assertNotNull(hm.getString("string"));
        Assert.assertNull(hm.getSubMap("submap").getString("newsubkey"));

        hm.mergeMap(oride);

        Assert.assertEquals("subVal", hm.getSubMap("newSub").getString("subKey"));
        Assert.assertNull(hm.getString("string"));
        DefaultingMap nsm = hm.getSubMap("submap");
        Assert.assertEquals("ns", nsm.getString("newsubkey"));
        Assert.assertNull(nsm.getString("smstr"));
    }
    

    @Test 
    public void testFlatten() {
        ArrayList<Object> topList = new ArrayList<Object>();
        ArrayList<String> lst = new ArrayList<String>();
        lst.add("f1");
        lst.add("f2");
        topList.add(lst);
        lst = new ArrayList<String>();
        lst.add("f1");
        lst.add((String)null);
        lst.add("f4");
        topList.add(lst);
        DefaultingMap hm = new DefaultingMap();
        hm.put("schema", topList);
        
        Set<String> sch = hm.getFlattenedList("schema");
        Assert.assertEquals(3, sch.size());
        Assert.assertTrue(sch.contains("f1"));
        Assert.assertTrue(sch.contains("f2"));
        Assert.assertFalse(sch.contains("f3"));
        Assert.assertTrue(sch.contains("f4"));
        
        sch = hm.getFlattenedList("notthere");
        Assert.assertNull(sch);
    }
    
    @Test
    public void testDouble() {
        DefaultingMap dm = makeMap();
        
        Assert.assertEquals(765.0, dm.getDouble("snum", -1.0), .0001);
        Assert.assertEquals(-1.0, dm.getDouble("xnum", -1.0), .0001);
        Assert.assertEquals(-1.0, dm.getDouble("string", -1.0), .0001);
        Assert.assertEquals(567.0, dm.getDouble("long", -1.0), .0001);

    }

    @Test
    public void testMergeSubMap() {
        HashMap<String,Object> sub = new HashMap<String,Object>();
        sub.put("s1", "v1");
        sub.put("ov", "ov1");
        ArrayList<HashMap<String,Object>> theList = 
                new ArrayList<HashMap<String,Object>>();
        theList.add(sub);
        sub = new HashMap<String,Object>();
        sub.put("t2", "vt2");
        sub.put("ov", "over");
        theList.add(sub);
        DefaultingMap dm = new DefaultingMap();
        dm.put("theMap", theList);

        DefaultingMap outMap = dm.getSubMap("theMap");

        Assert.assertEquals("v1", outMap.get("s1"));
        Assert.assertEquals("over", outMap.get("ov"));
        Assert.assertEquals("vt2", outMap.get("t2"));

    }

}
