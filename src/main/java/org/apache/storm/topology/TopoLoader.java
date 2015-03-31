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


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import org.apache.storm.topology.builder.ILoader;
import org.apache.storm.topology.builder.ITopologyConfigurator;
import org.apache.storm.topology.builder.Loader;
import org.apache.storm.topology.builder.Loader.LoadFailure;
import org.apache.utils.DefaultingMap;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.generated.TopologyInitialStatus;


/**
 * This is a java main class that reads in a yaml file that
 * describes a topology, constructs a toplogy using that
 * configuration, and submits the topology to storm to run.
 */
public class TopoLoader implements ITopologyConfigurator {
    
    @Option(name="--help", aliases={"-h"}, usage="print help message")
    private boolean _help = false;
    
    @Option(name="--name", aliases={"-n"}, usage="the name of the topology", metaVar="Name")
    private String _topologyName = null;

    @Option(name="--workers", aliases={"-w"}, usage="number of workers")
    private int _workers = -1;
    
    @Option(name="--local", aliases={"-l"}, usage="run in local mode for this many seconds", metaVar="seconds")
    private int _localSecs = 0;

    @Option(name="--overrides", metaVar="overrides", usage="comma-separated list of yaml files to update main yaml")
    private String _overrides = null;
    
    @Option(name="--maxparallel", aliases={"-p"},  usage="topology max parallelism")
    private int _maxParallel = -1;

    @Option(name="--debug", aliases={"-d"},  usage="turn on debug level logging")
    private boolean _debug = false;
    
    @Option(name="--dryrun",  usage="Dryrun. Build topology but don't submit")
    private boolean _dryrun = false;
    
    @Option(name="--inactive", aliases={"-i"}, usage="Inactive. Submit topology but don't activate")
    private boolean _inactive = false;

    @Argument
    private List<String> _args = new ArrayList<String>();
    
    protected final Config stormConf;
    private List<String> topoUsers;
    
    public TopoLoader() {
        stormConf = new Config();
        topoUsers = null;
    }
    
    
    /*
     * This function returns the Loader
     */
    @Override
    public ILoader getLoader(DefaultingMap conf) throws LoadFailure{
        return new Loader(conf, this);
    }
    
    /**
     * This is essentially "Main". Parse the command line. Read the topology
     * yaml and any overrides. Process the final config and submit the topology.
     * @param args - Command line arguments.
     * @return - Return nonzero on failure.
     * @throws InterruptedException
     * @throws LoadFailure 
     */
    public int runLoader(String[] args) throws InterruptedException, LoadFailure {
        int ret = 0;
        
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(80);
        
        try {
            parser.parseArgument(args);
        } catch( CmdLineException e ) {
            System.err.println(e.getMessage());
            _help = true;
            ret = 1;
        }
        
        if ((_args.size() <= 0) || "help".equalsIgnoreCase(_args.get(0))) {
            _help = true;     
        }
        if (_help) {
            System.err.println("storm jar <path> org.apache.storm.topology.TopoLoader [options] <yaml path>");
            parser.printUsage(System.err);
            System.err.println();
            return ret;
        }
        
        // Read the main yaml file.
        Map<String,Object> yaml = readYaml(_args.get(0));
        if (yaml == null) {
            return 1;
        }

        // Update the yaml just read with any environment-specific updates.
        if (!processOverrides(_overrides, yaml)) {
            // Something broke handling overrides.
            return 1;
        }

        if (!patchSchemas(yaml)) {
            // A problem with patching schemas
            return 1;
        }

        DefaultingMap conf = new DefaultingMap((Map<String,Object>)yaml);
        if (_localSecs > 0) {
            // If we're going to run locally, restrict parallelism
            conf.put(DefaultingMap.GLOBALPREFIX+"parallelism", 1);
        }
        if (_topologyName == null) {
            _topologyName = conf.getString("topologyname");
            if (_topologyName == null) {
                System.err.println("A topology name must be supplied either in yaml or command line");
                return 1;
            }
        }
        
        ILoader tLoader = getLoader(conf);
        StormTopology topology = tLoader.getTopology();
        SubmitOptions submitOptions = new SubmitOptions(_inactive ? TopologyInitialStatus.INACTIVE : TopologyInitialStatus.ACTIVE);
        if (_workers < 0) {
            _workers = conf.getInt("workers", 1);
        }
        stormConf.setNumWorkers(_workers);
        String commaUserList = conf.getString("topoUsers");
        if((commaUserList!=null) && (!commaUserList.trim().isEmpty())){
            String users[] = commaUserList.trim().split(",");
            
            if (users.length > 0) {
                topoUsers = Arrays.asList(users);
            }
        }
        
        // Should some workers be spread across nodes?
        List<String> spreads;
        if ((spreads = tLoader.getSpreads()) != null) {
            // TODO: Was this a local enhancement?
            // stormConf.put(Config.TOPOLOGY_SPREAD_COMPONENTS, spreads);
        }

        if (_debug) {
            stormConf.put(Config.TOPOLOGY_DEBUG, true);
        }

        if (_maxParallel > 0) {
            stormConf.setMaxTaskParallelism(_maxParallel);
        }

        if (_dryrun) {
            System.out.println("Dryrun. Skipping topology start");
        }
        else if (_localSecs > 0) {
            LocalCluster cluster = new LocalCluster();
            try {
                System.out.println("Submitting topology " + _topologyName + " locally");
                cluster.submitTopology(_topologyName, stormConf, topology);
                Thread.sleep(TimeUnit.SECONDS.toMillis(_localSecs));
                System.out.println("Killing topology");
            } catch (Exception e) {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
            cluster.shutdown();
        }
        else {
            System.out.println("Submitting topology " + _topologyName);
            try {
                if (topoUsers != null) {
                    // TODO: Was this a local enhancement?s
                    // stormConf.put(Config.TOPOLOGY_USERS, topoUsers);
                }
                StormSubmitter.submitTopology(_topologyName, stormConf, topology, submitOptions);
            } catch (Exception e) {
                System.out.println("Error submitting topology: " + e.getMessage());
                e.printStackTrace();
            }
        }
        
        return 0;
        
    }
    
    /**
     * Merge two yaml maps. Use the "override" map to replace or remove sections in "yaml."
     * For each submap in override:
     * (1) If override submap contains "deleteSection: true", the corresponding
     *     section submap in yaml is deleted.
     * (2) If override submap contains "replaceSection: true", the submap from override
     *     replaces the submap in yaml.
     * (3) Otherwise, everything in the overrides submap is added to the corresponding yaml
     *     submap, replacing existing values. If the submap didn't exist in yaml, it's added.
     *     Submaps are updated recursively.
     * @param yaml
     * @param override
     */
    public static void mergeYaml(Map<String,Object> yaml, Map<String,Object> override) {

        for (Map.Entry<String,Object> e : override.entrySet()) {
            String k = e.getKey();
            Object v = e.getValue();
            if (v instanceof Map<?,?>) {
                @SuppressWarnings("unchecked")
                Map<String,Object> subMap = (Map<String,Object>)v;

                if (isTrue(subMap.get("deleteSection"))) {
                    yaml.remove(k);
                }
                else {
                    Object ov = yaml.get(k);

                    if (isTrue(subMap.get("replaceSection"))) {
                        subMap.remove("replaceSection");
                        yaml.put(k, subMap);
                    }
                    else if (!(ov instanceof Map<?,?>)) {
                        yaml.put(k, subMap);
                    }
                    else {
                        @SuppressWarnings("unchecked")
                        Map<String,Object> oMap = (Map<String,Object>)ov;
                        mergeYaml(oMap, subMap);
                    }
                }
            }
            else {
                yaml.put(k, v);
            }
        }
    }

    /**
     * Update the yaml with any overrides specified on command line
     * @param overrides - Comma-separated list of yaml files to use to update yaml
     * @param yaml - The yaml to update
     * @return true if no errors encountered.
     */
    public static boolean processOverrides (String overrides, Map<String,Object> yaml) {
        if ((overrides == null) || overrides.isEmpty()) {
            return true;
        }
        for (String orFile : overrides.split(",")) {
            Map<String,Object> ory = readYaml(orFile);
            if (ory == null) {
                return false;
            }
            mergeYaml(yaml, ory);
        }
        return true;
    }
    
    /**
     * Process "listpatch" section of the yaml, if it exists.
     * This is used to modify schemas in the yaml. Generally the modifications
     * come from an override. This is used mostly in smoke test.
     * @param yaml
     * @return true if no errors found
     */
    @SuppressWarnings("unchecked")
    public static boolean patchSchemas(Map<String,Object> yaml) {
        Object o = yaml.get("listpatch");
        if (!(o instanceof Map<?,?>)) {
            // No patches found, so nothing to do.
            return true;
        }

        Map<String,Object> patches = (Map<String,Object>)o;
        for (Map.Entry<String,Object>e: patches.entrySet()) {
            o = e.getValue();
            if (!(o instanceof Map<?,?>)) {
                continue;
            }
            DefaultingMap patcher = new DefaultingMap((Map<String,Object>)o);
            
            String patchTarget = e.getKey();
            Map<String,Object> tgt = yaml;

            String sects[] = patchTarget.split(">");
            String finalSect = sects[sects.length-1];
            for (int i = 0; i < sects.length - 1; ++i) {
                String sect = sects[i];
                o = tgt.get(sect);
                if (!(o instanceof Map<?,?>)) {
                    // Everything down to the final item must be a map.
                    System.err.println("listpatch section " + patchTarget + " not found");
                    return false;
                }
                tgt = (Map<String,Object>)o;
            }
            
            // So now tgt is the map containing the actual schema.
            // finalSect is the name of the schema within that map.
            
            // Get the schema
            DefaultingMap dfTgt = new DefaultingMap(tgt);
            Set<String> schema = dfTgt.getFlattenedList(finalSect);

            if (schema == null) {
                // If the schema doesn't exist, adds are still reasonable.
                // Start with an empty schema
                schema = new HashSet<String>();
            }

            // Now we have the schema as a set in schema.
            // Now look for add or remove sections.
            Set<String>additions = patcher.getFlattenedList("add");
            if (additions != null) {
                // Process additions
                schema.addAll(additions);
            }
            Set<String>removals = patcher.getFlattenedList("remove");
            if (removals != null) {
                // Process additions
                schema.removeAll(removals);
            }

            // Now put the modified schema back into the original location.
            tgt.put(finalSect, new ArrayList<String>(schema));
        
        }
        return true;
    }

    /**
     * Read and parse a yaml file
     * @param fName - Path to file
     * @return - Map from the yaml, or null if file can't be loaded.
     */
    @SuppressWarnings("unchecked")
    public static Map<String,Object> readYaml(String fName) {        
        File yamlPath = new File(fName);
        InputStream yamlReader = null;
        try {
            yamlReader = new FileInputStream(yamlPath);
        } catch (FileNotFoundException e) {
            System.err.println("Can't open " + yamlPath);
            return null;
        }

        Yaml yaml = new Yaml(new SafeConstructor());
        Object y = null;
        try {
            y = yaml.load(new InputStreamReader(yamlReader));
        } finally {
            try {
                yamlReader.close();
            } catch (IOException e) {
                // Boreing
            }
        }

        if (!(y instanceof Map<?,?>)) {
            System.err.println("Error reading yaml file " + yamlPath);
            return null;
        }

        return (Map<String,Object>)y;
    }

    /**
     * Utility function. Return false if the value is null or not a Boolean, 
     * otherwise the Boolean value.
     * @param v
     * @return
     */
    protected static boolean isTrue(Object v) {
        return ((v != null) && (Boolean)v);
    }


    // From ITopologyConfigurator:
    @Override
    public Config getStormConfig() {
        return stormConf;
    }

    // From ITopologyConfigurator:
    @Override 
    public String getTopologyName() {
        return _topologyName;
    }

    /**
     * @param args
     * @throws InterruptedException 
     * @throws LoadFailure 
     */
    public static void main(String[] args) throws InterruptedException, LoadFailure {
        // SpringContextFactory.init();
        TopoLoader me = new TopoLoader();
        int retval = me.runLoader(args);
        System.exit(retval);

    }

}
