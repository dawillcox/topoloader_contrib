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

import java.lang.NumberFormatException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * For convenience of modules that interpret yaml files that define
 * a topology (and maybe for other things), this is a wrapper for
 * a HashMap that supports coercion of values and default values
 * to return if the requested value is missing or unconvertible.
 */
public class DefaultingMap extends HashMap<String, Object> {
    protected static final Logger logger = LoggerFactory.getLogger(DefaultingMap.class);
    private static final long serialVersionUID = 4866488686159859500L;
    public static final String GLOBALPREFIX = "g.";

    /**
     * Create a DefaultingMap that's a copy of another map.
     * @param map - Make a new DefaultingMap constructed from this map.
     */
    public DefaultingMap(Map<String,Object> map) {
        super(map);
    }

    /**
     * Create a new, empty DefaultingMap.
     */
    public DefaultingMap() {
        super();
    }

    /**
     * Generates a set of unique strings when a configuration
     * contains a tree of lists. This traverses the tree
     * walking each list it finds. All non-list items are
     * converted to String. The result is the unique
     * set of all such strings.
     * @param section - Name of the root of the tree in the map.
     * @return - Unique set of resulting strings, or null if there is no section.
     */
    @SuppressWarnings("unchecked")
    public Set<String> getFlattenedList(String section) {
        HashSet<String> theSet = null;

        Object fList = get(section);
        if (fList instanceof List<?>) {
            theSet = new HashSet<String>();
            doFlatten(theSet, (List<Object>) fList);
        }

        return theSet;
    }
    
    /**
     * Internal worker for getFlattenedList(). Walks a list accumulating
     * strings and recursively calling itself to walk child lists.
     * @param oSch - Set of strings being accumulated
     * @param iSch - List to walk
     */
    @SuppressWarnings("unchecked")
    protected static void doFlatten(HashSet<String> oSch, List<Object> iSch) {

        for (Object o : iSch) {
            if (o == null) {
                continue;
            } else if (o instanceof List<?>) {
                doFlatten(oSch, (List<Object>)o);
            }
            else {
                oSch.add(o.toString().trim());
            }
        }
    }

    /**
     * Basic defaulting "get". Return the item as it is found.
     * Return dflt if the item isn't found. There's no attempt
     * to confirm or coerce the type of the value if found.
     * @param <T> - Type to return
     * @param key - Key to look up
     * @param dflt - Value to return if no item found.
     * @return - Either value from map or dflt
     */
    @SuppressWarnings("unchecked")
    public <T> T get(String key, T dflt) {
        Object o = get(key);
        if (o == null) {
            return dflt;
        }
        else {
            return (T)(o);
        }
    }

    /**
     * Look for a value in the map and coerce it to a Double.
     * Return dflt if no value is found or the coercion fails.
     * @param key - Name to look up.
     * @param dflt - Default value if no value found
     * @return - Found value or dflt
     */
    public double getDouble(String key, double dflt) {
        Object o = get(key);
        if (o == null) {
            return dflt;
        }
        if (o instanceof Number) {
            return ((Number)o).doubleValue();
        }

        try {
            return Double.parseDouble(o.toString());
        } catch (NumberFormatException e) {
            logger.warn("Number format exception for {}:{}", key, o);
            return dflt;
        }
    }

    /**
     * Look for a value in the map and coerce it to a Long.
     * Return dflt if no value is find or the coercion fails.
     * @param key - Name to look up.
     * @param dflt - Default value if no value found
     * @return - Found value or dflt
     */
    public long getLong(String key, long dflt) {
        Object o = get(key);
        if (o == null) {
            return dflt;
        }
        if (o instanceof Long) {
            return ((Long)o).longValue();
        }
        if (o instanceof Integer) {
            return ((Integer)o).longValue();
        }
        // OK, if it's really a string, assume it can be parsed.
        try {
            return Long.parseLong(o.toString());
        } catch (NumberFormatException e) {
            logger.warn("Number format exception for {}:{}", key, o);
            return dflt;
        }
    }

    /**
     * Look for a value in the map and coerce it to an Integer.
     * Return dflt if no value is find or the coercion fails.
     * @param key - Name to look up.
     * @param dflt - Default value if no value found
     * @return - Found value or dflt
     */
    public int getInt(String key, int dflt) {
        long val = getLong(key, (long)dflt);
        return (int)val;
    }

    /**
     * Look for a value in the map and coerce it to a String.
     * Return dflt if no value is find or the coercion fails.
     * @param key - Name to look up.
     * @param dflt - Default value if no value found
     * @return - Found value or dflt, trimmed
     */
    public String getString(String key, String dflt) {
        Object o = get(key);
        if (o == null) {
            return dflt == null ? null : dflt;
        }
        else {
            return o.toString();
        }
    }

    /**
     * Look for a value in the map and coerce it to a boolean.
     * Return dflt if no value is find or the coercion fails.
     * @param key - Name to look up.
     * @param dflt - Default value if no value found
     * @return - Found value or dflt
     */
    public boolean getBool(String key, boolean dflt) {
        Object o = get(key);
        if (o == null) {
            return dflt;
        }

        if (o instanceof Boolean) {
            return (Boolean)o;
        }

        if (o instanceof Number) {
            return (((Number)o).longValue() != 0);
        }

        return Boolean.parseBoolean(o.toString());
    }

    /**
     * Look for a value in the map and coerce it to a String.
     * Return null if nothing found. This is equivalent to
     * getString(key, null)
     * @param key - Name to look up.
     * @return - Found value or null
     */
    public String getString(String key) {
        return getString(key, null);
    }

    /**
     * Update this map with the content of another map.
     * Anything in updater should be merged into this map.
     * If updater's value is null, delete the corresponding
     * element in this map. If the values are maps,
     * do a recursive update. If updater has 'replaceall'
     * set to true, replace content of this with updater.
     * @param updater - Map use to update this map.
     */
    public void mergeMap(DefaultingMap updater) {
        if (updater.getBool("replaceall", false)) {
            // replaceall in updater is true, meaning we should
            // replace everything, not merge.
            this.clear();
            this.putAll(updater);
            return;
        }

        for (Map.Entry<String,Object> e : updater.entrySet()) {
            String k = e.getKey();
            Object v = e.getValue();
            if (v instanceof DefaultingMap) {
                DefaultingMap nMap = (DefaultingMap)v;
                DefaultingMap oMap = this.getSubMap(k);
                if (oMap == null) {
                    this.put(k, nMap);
                }
                else {
                    oMap.mergeMap(nMap);
                }
            }
            else if (v instanceof Map<?,?>) {
                @SuppressWarnings("unchecked")
                DefaultingMap nMap = new DefaultingMap((Map<String,Object>)v);
                DefaultingMap oMap = this.getSubMap(k);
                if (oMap == null) {
                    oMap = nMap;
                }
                else {
                    oMap.mergeMap(nMap);
                }
                // We put oMap back into the parent map even if it wasn't null
                // because it's a new object now.
                this.put(k, oMap);
            }
            else {
                this.put(k, v);
            }
        }
    }

    /**
     * Get a child configuration from a parent map as a DefaultingMap.
     * An important aspect of this: Any entry whose key starts with
     * GLOBALPREFIX ("g.") will be pushed down to the child map. That means
     * anything defined as "g.x" will also be visible to all
     * child configurations.
     * getSubMap() handles three cases:
     * (1) If the value is already a DefaultingMap, it's returned as is.
     * (2) If the value is a regular Map, it's turned into a DefaultingMap.
     * (3) If the value is a list of Maps, a new DefaultingMap is constructed
     *     by merging all of the individual Maps; the key/values of each
     *     is added to the new DefaultingMap. If any key appears more than once,
     *     the one from the last will be used. 
     * In either (2) or (3), g. globals are copied to the new DefaultingMap,
     * and the new DefaultingMap replaces the existing value in the parent
     * DefaultingMap.
     * @param key - Name to look up
     * @return - Specified value as a DefaultingMap
     */
    public DefaultingMap getSubMap(String key) {
        Object o = get(key);
        DefaultingMap ret = null;

        if (o instanceof DefaultingMap) {
            ret = (DefaultingMap)o;
        }
        else if (o instanceof Map<?,?>) {
            @SuppressWarnings("unchecked")
            Map<String,Object> map = (Map<String, Object>)o;
            ret = new DefaultingMap(map);
            put(key, ret);
        }
        else if (o instanceof List<?>) {
            // Rather than a single map, this is a list of maps to merge.
            ret = new DefaultingMap();
            @SuppressWarnings("unchecked")
            List<Object> theList = (List<Object>)o;
            for (Object ent: theList) {
                if (ent instanceof Map<?,?>) {
                    @SuppressWarnings("unchecked")
                    Map<String,Object>subMap = (Map<String,Object>)ent;
                    ret.putAll(subMap);
                }
                else {
                    return null;
                }
            }
            put(key, ret);
        }
        else {
            return null;
        }

        // Push any global settings from this map to the child map.
        for (Map.Entry<String,Object> entry : entrySet()) {
            String k = entry.getKey();
            if (k.startsWith(GLOBALPREFIX) && !key.equals(k)) {
                ret.put(k, entry.getValue());
            }
        }

        return ret;
    }
}
