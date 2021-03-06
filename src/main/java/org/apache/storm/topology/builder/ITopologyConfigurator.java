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

public interface ITopologyConfigurator {

    /**
     * Get the storm topology configuration. (So it can be modified.)
     * @return - Config that will be used when submitting to storm.
     */
    public Config getStormConfig();
    
    /**
     * Name of topology to be submitted.
     * @return
     */
    public String getTopologyName();
    
    /**
     * Get the Loader.
     * @return
     */
    public ILoader getLoader(DefaultingMap conf) throws LoadFailure;

}
