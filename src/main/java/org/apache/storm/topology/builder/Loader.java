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

import backtype.storm.generated.StormTopology;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.utils.DefaultingMap;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Loader implements ILoader {
    private static final Logger Logger = LoggerFactory.getLogger(Loader.class);

    public final String spreadKey = "spreadworkers";

    protected final TopologyBuilder builder;
    protected final List<String> spreadObjects;

    // Indexes of subfields in input spec.
    protected static final int INPUT_SOURCE_IDX = 0;
    protected static final int INPUT_STREAM_IDX = 1;
    protected static final int INPUT_GROUPING_IDX = 2;
    protected static final int INPUT_FIELDS_IDX = 3;

    protected Set<String> activeMods = new HashSet<String>();
    protected Map<String,DefaultingMap> spoutSpecs = null;
    protected Map<String,DefaultingMap> boltSpecs = null;

    protected enum GroupingType {shuffle, all, fields, none, global, direct, localOrShuffle, custom};

    protected static class InputSpec implements Serializable {
        private static final long serialVersionUID = -7139082426986672995L;
        protected final GroupingType grouping;
        protected final String source;
        protected final String streamId;
        protected final String fields;
        protected final DefaultingMap customSpec;

        public InputSpec(DefaultingMap conf) {
            customSpec = conf;
            source = conf.getString("component", "<none>");
            streamId = conf.getString("streamid", Utils.DEFAULT_STREAM_ID);
            grouping = GroupingType.custom;
            fields = null;
        }

        public InputSpec(String inSpec) {
            customSpec = null;
            String[] specFields = inSpec.split(":");

            source = specFields[INPUT_SOURCE_IDX].trim();
            if (source.isEmpty()) {
                throw new IllegalArgumentException ("Input source must be set: " + inSpec);
            }

            if ((specFields.length > INPUT_STREAM_IDX) && !specFields[INPUT_STREAM_IDX].isEmpty()) {
                streamId = specFields[1].trim();
            }
            else {
                streamId = Utils.DEFAULT_STREAM_ID;
            }

            if ((specFields.length > INPUT_GROUPING_IDX) && !specFields[INPUT_GROUPING_IDX].isEmpty()) {
                try {
                    grouping = GroupingType.valueOf(specFields[INPUT_GROUPING_IDX].trim());
                }
                catch (Exception e) {
                    throw new IllegalArgumentException("Invalid grouping type in " + inSpec);
                }
            }
            else {
                grouping = GroupingType.shuffle;
            }

            if (specFields.length > INPUT_FIELDS_IDX) {
                fields = specFields[INPUT_FIELDS_IDX].trim();
            }
            else {
                fields = "";
            }

            if ((grouping == GroupingType.fields) && fields.isEmpty()) {
                throw new IllegalArgumentException("Fields must be supplied for fields grouping: " + inSpec);
            }
        }

        public GroupingType getGrouping() {
            return grouping;
        }

        public String getStreamId() {
            return streamId;
        }

        public String getSource() {
            return source;
        }

        public Fields getFields() {
            return new Fields(fields.split(","));
        }

        public DefaultingMap getCustomSpec() {
            return customSpec;
        }
    }


    /**
     * Create an object using the given name and DefaultingMap parameters.
     * The class of the object is named by element "class" in the DefaultingMap.
     * The allocated object can be any type as long as it provides the necessary
     * constructor. If the constructor is missing then default constructor will be used to 
     * create the object. It's up to the caller to verify that the created object
     * is actually of the expected type.
     * 
     * @param name - A name for the object. This has no meaning here, it's
     *               provided to the class constructor to use as it wishes.
     * @param params - DefaultingMap parameters. The name of the class to
     *               instantiate is the "class" element in the DefaultingMap.
     *               Everything else in DefaultingMap is passed through to
     *               the class's constructor.
     * @return Object - The loaded object
     * @throws LoadFailure - Object can't be created.
     */
    public static Object loadAndBuild(String name, DefaultingMap params) throws LoadFailure {
        Object theMod = null;

        String className = params.getString("builder", null);
        if (className != null) {
            return loadAndBuildBuilder(name, params, className);
        }

        className = params.getString("class", null);
        if (className == null) {
            Logger.error("No class defined for {}", name);
            return null;
        }

        try {
            Class<?> classType = Class.forName(className);
            Constructor<?> ctor = null;
            try {

                ctor = classType.getDeclaredConstructor(String.class, DefaultingMap.class);
                ctor.setAccessible(true);           
                theMod = ctor.newInstance(name, params);

            } catch (NoSuchMethodException e) {
                //create object using the default constructor
                ctor = classType.getDeclaredConstructor();
                ctor.setAccessible(true);           
                theMod = ctor.newInstance();
            }
        } catch (Exception e) {
            throw new LoadFailure(className, name, e);
        }

        return theMod;
    }

    /**
     * This is a variation of loadAndBuild() that allocates the module
     * using a static builder method.
     * @param name - Name of module
     * @param params - DefaultingMap specification
     * @param builderDef - Builder definition string: &lt;ClassName&gt;[:&lt;MethodName&gt;]
     * @return the created module
     * @throws LoadFailure if the module spec is bad
     */
    protected static Object loadAndBuildBuilder(String name, DefaultingMap params, String builderDef) throws LoadFailure {
        String builderParts[] = builderDef.split(":");
        String className = builderParts[0];
        String builderMethod = (builderParts.length > 1) ? builderParts[1] : "builder";

        try {
            Class<?> classType = Class.forName(className);
            Method builder = classType.getDeclaredMethod(builderMethod, String.class, DefaultingMap.class);
            builder.setAccessible(true);
            return builder.invoke(null, name, params);
        } catch (Exception e) {
            throw new LoadFailure(builderDef, name, e);
        }

    }

    /**
     * Version of addBolt that uses this objects builder.
     * @param boltName - Name of bolt in topoogy
     * @param conf - Bolt's configuration
     * @param topoCfg - Topology configurator
     * @throws LoadFailure - If bolt can't be loaded
     */
    protected void addBolt(String boltName, DefaultingMap conf, ITopologyConfigurator topoCfg) throws LoadFailure {
        addBolt(builder, boltName, conf, topoCfg);
    }

    /**
     * Instantiate a bolt defined by the given bolt configuration.
     * Add it to the TopologyBuilder with options (inputs, parallelism)
     * as defined in the configuration.
     * Note: If parallelism is explicitly set to zero, the bolt will be
     * skipped.
     * 
     * @param tBldr - The topology
     * @param boltName - The name of the bolt
     * @param conf - Configuration describing the bolt.
     * @param topoCfg - Topology configurator
     * @throws LoadFailure  - If bolt can't be loaded
     */
    public void addBolt(TopologyBuilder tBldr, String boltName, DefaultingMap conf, ITopologyConfigurator topoCfg) throws LoadFailure {
        
        int parallelism = conf.getInt("parallelism", 1);
        if (parallelism <= 0) {
            Logger.info("Bolt {} disabled", boltName);
            return;
        }
        
        Object o = Loader.loadAndBuild(boltName, conf);

        // Give the bolt a chance to add anything needed to the topology configuration.
        moduleTopoConfig (o, topoCfg);

        BoltDeclarer declarer = null;
        if (o instanceof IRichBolt) {
            declarer = tBldr.setBolt(boltName, (IRichBolt)o, parallelism);
        } else if (o instanceof IBasicBolt) {
            declarer = tBldr.setBolt(boltName, (IBasicBolt)o, parallelism);
        } else {
            throw new IllegalArgumentException("Bolt " + boltName + " doesn't implement IRichBolt or IBasicBolt");
        }

        o = conf.get("inputs");
        if (o instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<InputSpec>inputs = (List<InputSpec>) o;

            for (InputSpec inspec : inputs) {
                chainInput(boltName, declarer, inspec);
            }
        }
    }


    /**
     * Add the given input to the bolt
     * @param boltName - name of bolt
     * @param declarer - bolt declarer
     * @param inputSpec - input specification
     * @throws LoadFailure - If bolt can't be loaded
     */
    protected void chainInput(String boltName, BoltDeclarer declarer, InputSpec inputSpec) throws LoadFailure {
        GroupingType grouping = inputSpec.getGrouping();
        String sourceModule = inputSpec.getSource();

        if (!activeMods.contains(sourceModule)) {
            Logger.warn("Dropping input from {} to {}", sourceModule, boltName);
            return;
        }
        switch (grouping) {
        case shuffle:
            declarer.shuffleGrouping(sourceModule, inputSpec.getStreamId());
            break;

        case all:
            declarer.allGrouping(sourceModule, inputSpec.getStreamId());
            break;

        case fields:
            declarer.fieldsGrouping(sourceModule, inputSpec.getStreamId(), inputSpec.getFields());
            break;

        case localOrShuffle:
            declarer.localOrShuffleGrouping(sourceModule, inputSpec.getStreamId());
            break;

        case direct:
            declarer.directGrouping(sourceModule, inputSpec.getStreamId());
            break;

        case global:
            declarer.globalGrouping(sourceModule, inputSpec.getStreamId());
            break;

        case none:
            declarer.noneGrouping(sourceModule, inputSpec.getStreamId());
            break;

        case custom:
            Object customGroup = Loader.loadAndBuild("customInput", inputSpec.getCustomSpec());
            if (!(customGroup instanceof CustomStreamGrouping)) {
                throw new IllegalArgumentException(String.format("Bolt %s input must implement CustomStreamGrouping", boltName));
            }
            declarer.customGrouping(inputSpec.getSource(), inputSpec.getStreamId(), (CustomStreamGrouping)customGroup);
            break;

        default:
            Logger.warn("Invalid grouping {} for {}", grouping, boltName);
            throw new IllegalArgumentException(String.format("Invalid grouping %s for %s", grouping, boltName));
        }

    }

    /**
     * Version of addSpout() that uses built-in builder.
     * @param spoutName - Name of spout in toplogy
     * @param conf - Configuration describing spout
     * @param topoCfg Topology configuator
     * @throws LoadFailure - If spout couldn't be created
     */
    protected void addSpout(String spoutName, DefaultingMap conf, ITopologyConfigurator topoCfg) throws LoadFailure {
        addSpout(builder, spoutName, conf, topoCfg);
    }
    /**
     * Instantiate a spout defined by the given spout configuration.
     * Add it to the TopologyBuilder with options (parallelism)
     * as defined in the configuration.
     * Note: If parallelism is explicitly set to zero, the spout will be
     * skipped.
     * 
     * @param tBldr - TopologyBuilder to use
     * @param spoutName - Name of the spout
     * @param conf - Configuration describing the spout.
     * @param topoCfg - Topology configurator
     * @throws LoadFailure  - If spout couldn't be created
     */
    public void addSpout(TopologyBuilder tBldr, String spoutName, DefaultingMap conf, ITopologyConfigurator topoCfg) throws LoadFailure {
        
        int parallelism = conf.getInt("parallelism", 1);
        if (parallelism <= 0) {
            Logger.info("Spout {} disabled", spoutName);
            return;
        }
        
        Object o = Loader.loadAndBuild(spoutName, conf);
        if (!(o instanceof IRichSpout)) {
            throw new IllegalArgumentException("Spout " + spoutName + " doesn't implement IRichSpout");
        }
        IRichSpout spout = (IRichSpout)o;

        @SuppressWarnings("unused")
        SpoutDeclarer declarer = tBldr.setSpout(spoutName, spout, parallelism);

        // Give the spout a chance to add anything needed to the topology configuration.
        moduleTopoConfig (spout, topoCfg);
    }

    /**
     * If this loadable object is an ITopologyConfigure, give it the
     * opportunity to configure topology configuration and/or submit options.
     * @param module - The module that might be ITopologyConfigure
     * @param configurator - Configurator object
     */
    public static void moduleTopoConfig(Object module, ITopologyConfigurator configurator) {

        if (module instanceof ITopologyConfigure) {
            ITopologyConfigure cfger = (ITopologyConfigure)module;
            cfger.doStormConfig(configurator);
        }
    }

    /**
     * Return the schema from a section. For backwards compatibility, special case of getFlattenedList().
     * @param conf - DefaultingMap section with schema.
     * @return Set of strings in schema.
     */
    public static Set<String> getSchema(DefaultingMap conf) {
        return conf.getFlattenedList("schema");
    }

    /**
     * Do a topology-specific initialization of the storm Config.
     * @param configurator - Topology condfigurator
     * @param topoConf - Config for this module.
     * @return 0 on success, 1 on error.
     * @throws LoadFailure  - If object couldn't be created
     */
    public static int doCustomConfig(ITopologyConfigurator configurator, DefaultingMap topoConf) throws LoadFailure {
        Object o = topoConf.get("config");
        if (o == null) {
            return 0;
        }
        if (!(o instanceof Map<?,?>)) {
            System.err.println("Config is not a map");
            return 1;
        }
        @SuppressWarnings("unchecked")
        Map<String,Object> configs = (Map<String,Object>)o;

        for (Map.Entry<String,Object> e : configs.entrySet()) {
            Object c = e.getValue();
            if (!(c instanceof Map<?,?>)) {
                System.err.println("Invalid configurator");
                return 1;
            }

            @SuppressWarnings("unchecked")
            DefaultingMap cfg = new DefaultingMap((Map<String,Object>)c);
            String cfgName = e.getKey();

            o = Loader.loadAndBuild(cfgName, cfg);
            if (!(o instanceof ITopologyConfigure)) {
                System.err.println("Can't do configuration " + cfgName);
                return 1;
            }

            ITopologyConfigure cfger = (ITopologyConfigure)o;
            cfger.doStormConfig(configurator);

        }

        return 0;
    }

    /**
     * Version of constructor that allocates the TopologyBuilder.
     * This is the constructor normally used.
     * @param conf - Module configuration
     * @param topoCfg - Topology configurator
     * @throws LoadFailure  - If object couldn't be created
     */
    public Loader(DefaultingMap conf, ITopologyConfigurator topoCfg) throws LoadFailure {
        this(conf, new TopologyBuilder(), topoCfg);
    }

    /**
     * Create a TopologyBuilder object and populate it from the given
     * configuration. This instantiates all spouts and bolts, giving each
     * its own subsection to let it configure itself, adds each spout and
     * bolt to the topology with the appropriate parallelism setting,
     * and sets the inputs for each bolt as specified in the configuration.
     * This constructor is usually used for unit tests. It allows the test
     * to provide a mock builder.
     * 
     * @param conf - The DefaultingMap configuration.
     * @param builder - Topology builder to use to build topology.
     * @param topoCfg - Topology configurator
     * @throws LoadFailure - If object can't be created
     */
    @SuppressWarnings("unchecked")
    public Loader(DefaultingMap conf, TopologyBuilder builder, ITopologyConfigurator topoCfg) throws LoadFailure {

        Object o = conf.get("spouts");
        if (!(o instanceof Map<?,?>)) {
            throw new IllegalArgumentException("No spouts defined");
        }
        spoutSpecs = getModules((Map<String,Object>)o);
        activeMods.addAll(spoutSpecs.keySet());

        o = conf.get("bolts");
        if (!(o instanceof Map<?,?>)) {
            Logger.warn("No bolts defined");
            boltSpecs = new HashMap<String,DefaultingMap>();
        } else {
            boltSpecs = getModules((Map<String,Object>)o);
            activeMods.addAll(boltSpecs.keySet());
        }

        this.builder = builder;
        spreadObjects = new ArrayList<String>();

        for (Map.Entry<String, DefaultingMap> se : spoutSpecs.entrySet()) {
            DefaultingMap spoutMap = se.getValue();
            String spoutName = se.getKey();

            addSpout(spoutName, spoutMap, topoCfg);

            if (spoutMap.getBool(spreadKey, false)) {
                spreadObjects.add(spoutName);
            }
        }

        if (!conf.getBool("keeporphans", false)) {
            dropOrphans();
        }

        if (boltSpecs != null) {
            for (Map.Entry<String, DefaultingMap> be : boltSpecs.entrySet()) {
                DefaultingMap boltConf = be.getValue();
                String boltName = be.getKey();

                addBolt(boltName, boltConf, topoCfg);

                if (boltConf.getBool(spreadKey, false)) {
                    spreadObjects.add(boltName);
                }
            }
        }

        // Look for any non-spout/bolt modules for custom configuration.
        doCustomConfig(topoCfg, conf);
    }

    /**
     * Make a map of module specifications, either spouts or bolts.
     * @param modules - Map of module specifications from YAML
     * @return map of module names to configuration.
     */
    protected Map<String,DefaultingMap> getModules(Map<String,Object> modules) {

        Map<String,DefaultingMap> modSpecs = new HashMap<String,DefaultingMap>();
        for (Map.Entry<String, Object> se : modules.entrySet()) {
            String modName = se.getKey();
            Object s = se.getValue();
            if (!(s instanceof Map<?,?>)) {
                if (!modName.startsWith(DefaultingMap.GLOBALPREFIX)) {
                    Logger.error("malformed module specification for {} ignored", modName);
                }
                continue;
            }

            @SuppressWarnings("unchecked")
            DefaultingMap modMap = new DefaultingMap((Map<String,Object>)s);
            int parallelism = modMap.getInt("parallelism", 1);
            if (parallelism > 0) {
                Object i = modMap.get("inputs");
                if (i instanceof List<?>) {
                    // turn bolt input list into list of InputSpecs
                    @SuppressWarnings("unchecked")
                    List<Object> inputs = (List<Object>)i;
                    ArrayList<InputSpec> inputSpecs = new ArrayList<InputSpec>(inputs.size());
                    for (Object iobj : inputs) {
                        if (iobj instanceof String) {
                            InputSpec ispec = new InputSpec((String)iobj);
                            inputSpecs.add(ispec);
                        } else if (iobj instanceof Map<?,?>) {
                            // Presumed to be a custom grouping
                            @SuppressWarnings("unchecked")
                            DefaultingMap iconf = new DefaultingMap((Map<String,Object>)iobj);
                            inputSpecs.add(new InputSpec(iconf));
                        } else {
                            Logger.warn("Dropping {} as input to {}", iobj, modName);
                        }
                    }
                    modMap.put("inputs", inputSpecs);
                }
                modSpecs.put(modName, modMap);
            }
        }
        return modSpecs;
    }

    /**
     * Get the topology that was built.
     * @return StormTopology
     */
    @Override
    public StormTopology getTopology() {
        return builder.createTopology();
    }

    /**
     * Go through the bolts. Remove any inputs that aren't from
     * existing, active modules. If there are no remaining inputs
     * to the bolt, remove the bolt from the topology.
     */
    @SuppressWarnings("unchecked")
    protected void dropOrphans() {
        boolean didchange;
        do {
            didchange = false;
            Iterator<Entry<String,DefaultingMap>> biter = boltSpecs.entrySet().iterator();
            while (biter.hasNext()) {
                Entry<String,DefaultingMap> bspec = biter.next();
                String boltName = bspec.getKey();
                DefaultingMap conf = bspec.getValue();
                Object iobj = conf.get("inputs");
                if ((iobj instanceof List<?>) && (((List<String>)iobj).size() > 0)) {
                    // Go through inputs, remove any from disabled sources.
                    List<InputSpec> inputs = (List<InputSpec>)iobj;
                    Iterator<InputSpec> inpiter = inputs.iterator();
                    while (inpiter.hasNext()) {
                        InputSpec inspec = inpiter.next();
                        String source = inspec.getSource();
                        if (!activeMods.contains(source)) {
                            Logger.warn("Input to {} from disabled {} removed", boltName, source);
                            inpiter.remove();
                        }
                    }
                    if (inputs.size() > 0) {
                        // This bolt still has inputs from active modules, so go on to next.
                        continue;
                    }
                }

                // If we get here, this bolt no longer has inputs from active
                // modules. Delete it from the topology.
                Logger.warn("Removing bolt {} which has no inputs", boltName);
                biter.remove();
                activeMods.remove(boltName);
                didchange = true;
            }
        } while (didchange);
    }

    /**
     * Get list of spouts/bolts that should be spread across
     * processors. Returns null if there are none.
     * @return strings - List of workers to spread out.
     */
    @Override
    public List<String> getSpreads() {
        return (spreadObjects.size() > 0) ? spreadObjects : null;
    }

    public static class LoadFailure extends Exception {
        private static final long serialVersionUID = -1317670350831813808L;
        protected final String className;
        protected final String objName;

        public LoadFailure(String className, String objName, Exception e) {
            super(e);
            this.className = className;
            this.objName = objName;
        }

        @Override
        public String getMessage() {
            return String.format("Exception loading %s for %s: %s", className, objName, super.getMessage());
        }

    }
}
