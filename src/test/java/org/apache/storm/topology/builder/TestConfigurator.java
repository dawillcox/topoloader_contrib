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

import org.apache.storm.topology.builder.Loader.LoadFailure;
import org.apache.utils.DefaultingMap;

import backtype.storm.Config;
import org.apache.storm.topology.builder.ILoader;
import org.apache.storm.topology.builder.ITopologyConfigurator;

/**
 * Stub used in several unit tests
 */
public class TestConfigurator implements ITopologyConfigurator {
    
    protected final String topoName;
    
    public TestConfigurator(String tname) {
        topoName = tname;
    }
    
    public TestConfigurator() {
        this("testTopology");
    }

    protected Config cfg = new Config();
    @Override
    public Config getStormConfig() {
        return cfg;
    }
    @Override
    public String getTopologyName() {
        return topoName;
    }

    @Override
    public ILoader getLoader(DefaultingMap conf) throws LoadFailure {
        return null;
    }

    
}

