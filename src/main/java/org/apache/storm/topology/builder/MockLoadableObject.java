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

import backtype.storm.Config;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.WorkerTopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;

import org.apache.storm.topology.builder.ITopologyConfigurator;
import org.apache.storm.topology.builder.ITopologyConfigure;
import org.apache.utils.DefaultingMap;

/**
 * This is not, in itself, a unit test.  It's used in LoaderTest.
 * @author dwillcox
 *
 */
public class MockLoadableObject implements IRichBolt, IRichSpout, ITopologyConfigure, CustomStreamGrouping {
    private static final long serialVersionUID = 1384568161072706428L;
    protected final String myName;
    protected final String myVar;
    
    protected long nullValue[] = null;
    protected String schemaFields[] = null;
    protected final long tableMultiplier;
    protected final DefaultingMap theConf;

    public MockLoadableObject(String name, DefaultingMap conf) {
        myName = name;
        myVar = conf.getString("val", null);
        tableMultiplier = conf.getLong("multiplier", 2);
        theConf = conf;
    }

    public String myName() {
        return myName;
    }
    
    public String myVar() {
        return myVar;
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context,
            OutputCollector collector) {
        
    }

    @Override
    public void execute(Tuple input) {
        
    }

    @Override
    public void cleanup() {
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        
    }

    @Override
    public void close() {
        
    }

    @Override
    public void activate() {
        
    }

    @Override
    public void deactivate() {
        
    }

    @Override
    public void nextTuple() {
        
    }

    @Override
    public void ack(Object msgId) {
        
    }

    @Override
    public void fail(Object msgId) {
        
    }


    @Override
    public void doStormConfig(ITopologyConfigurator topoConf) {

        Config stormConf = topoConf.getStormConfig();

        for (Map.Entry<String, Object> e : theConf.entrySet()) {
            String k = e.getKey();
            if ((k != null) && !"name".equals(k) && !"class".equals(k)) {
                stormConf.put(k, e.getValue());
            }
        }

    }

    @Override
    public void prepare(WorkerTopologyContext context, GlobalStreamId stream,
            List<Integer> targetTasks) {        
    }

    @Override
    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        return null;
    }

}
