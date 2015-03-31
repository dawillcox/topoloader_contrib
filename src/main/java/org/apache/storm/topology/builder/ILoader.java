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

import java.util.List;
import backtype.storm.generated.StormTopology;


/**
 * This is an interface for the Loader used to load all the 
 * config parameters and return the StormTopology
 *
 */
public interface ILoader {
    
    /**
     * Get the topology that was built.
     * @return StormTopology
     */
    public StormTopology getTopology();

    /**
     * Get list of spouts/bolts that should be spread across
     * processors. Returns null if there are none.
     * @return List&lt;String&gt; - List of workers to spread out.
     */
    public List<String> getSpreads();

}
