This module provides a handy utility that lets you describe a storm configuration in a yaml file. 

# Main classes

## DefaultingMap

**DefaultingMap** is an extension of Map&lt;String,Object&gt; that adds some helper functions that are especially useful when 
the Map is describing a module configuration. For example, 

    String parm = m.getString(key,default) 

returns **default** if the key isn't found, and coerces the value to a String if it is found.

## Loader.loadAndBuild()

**Loader.loadAndBuild(String name, DefaultingMap conf)** instantiates a "loadable object." It first looks for "class" in the conf
map and takes that as a class path. It instantiates an object of that class using the class's constructor 

    public MyClass(String name, DefaultingMap conf){ ... };

The class is expected to do whatever configuration it needs using values from **conf**.

Then, finally, the **Loader** class reads a yaml file and builds the topology from it.

Note that both spouts and bolts are free to use **loadAndBuild()** to create submodules in the same way. 
The submodules just need to supply the same constructor.

## ITopologyConfigurator

If any of your spouts or bolts has a need to make a special modification of the configuration that will be used when submitting
the topology, it can do that by implementing the **ITopologyConfigure** interface. You can also do custom configurations by using another
class that implements **ITopologyConfigure** and listing them in the config yaml section; see below.

The class's:

    void doStormConfig(ITopologyConfigurator config);

method will be called to give the module the opportunity to adjust parameters that will be used when the topology is submitted to storm. The ITopologyConfigurator gives the method access to the name of the topology to be submitted, and the Config object that will be used in the submission.

# Yaml File Format

Briefly, the yaml file contains:

    spouts:
      aspout:
        class: <classpath of spout>
        parallelism: <n>
        [spread:true|false]
        ... Spout-specific parameters ...
      anotherspout:
        etc

    bolts:
      abolt:
        class: <classpath of bolt>
        parallelism: <n>
        [spread:true|false]
        inputs:  # A list of one or more inputs, each in one of the following forms:
          # In the following, <component> is the name of source spout or bolt,
          # <stream> is name of stream from that component, and <field> is
          # a field name. If <stream> is emitted, the default stream is used.
          - <component>:[<stream>]:all
          - <component>[:[<stream>][:shuffle]]    # Shuffle is default
          - <component>:[<stream>]:localOrShuffle
          - <component>:[<stream>]:shuffle:<field>,<field>...
          # Or, for customGrouping():
          - class: <classpath of customGrouping loadable object>
            component: <component>
            [streamid: <stream>]
            ... Other parameters for customGrouping
        ... Bolt-specific parameters ...

       # This is optional:
    configs:
      aconfig:
        class: <classpath>
        # Class must implement ITopologyConfigure.
        ... Other parameters.

Note that both spouts and bolts are free to use **loadAndBuild()** to create submodules in the same way. 
The submodules just need to supply the same constructor.

## Execution

Execute the loader as follows:

    storm jar <jarPath>  org.apache.storm.topology.TopoLoader [options] <pathToYaml>

Options are:

     --debug                : turn on debug-level logging
     --dryrun               : Dryrun. Build topology but don't submit
     --help (-h)            : print help message
     --inactive (-i)        : Inactive. Submit topology but don't activate
     --local (-l) seconds   : run in local mode for this many seconds
     --maxparallel (-p) N   : topology max parallelism
     --name (-n) Name       : the name of the topology
     --overrides overrides  : comma-separated list of yaml files to update main yaml
     --workers (-w) N       : number of workers
    


# Defining a Storm Topology With TopoLoader

## Overview

TopoLoader is a class in ystorm_contrib that lets you describe your storm topology using a YAML file. You’ll need to modify your existing spouts and bolts (and potentially other classes) to configure themselves using a map passed to the constructor (or create builders to instantiate objects using the same map), but once you’ve done that you can describe your topology and its configuration entirely in a YAML file. There’s no need to write a custom topology builder to instantiate and configure all of the components in your topology.
First we describe changes needed to let your components to work with TopoLoader. Then we describe how you can define your topology using a YAML file.

## Storm Components as Loadable Objects

TopoLoader uses the concept of a “loadable object.” Basically, a loadable object is something that can be instantiated and configure itself completely from a map of parameters passed to its constructor. The object can’t depend on some outside agent (e.g. a topology builder routine) to supply configuration settings; anything it needs must be in the configuration map.

### Defaulting Map

TopoLoader makes use of the DefaultingMap class. Configurations passed to loadable object constructors are in this form. DefaultingMap extends Map&lt;String,Object&gt;, so you can treat the configuration as a simple Map if you prefer. But DefaultingMap provides some wrapper methods that are handy when working with configuration parameters. For example:

    int aparam = map.getInt(key, default);
    String another = map.getString(otherkey,”defaultvalue”);

These and similar methods look up the key in the map. They attempt to coerce the value to the correct type, and will return the specified default if the value isn’t defined in the map.

### Loadable Objects

There are two ways to make an object “loadable” for use with TopoLoader. The module can implement a constructor of the form:

    public <class>(String name, DefaultingMap conf) { }

TopoLoader can instantiate the object using the above constructor, passing a name and the configuration map to the constructor to let the object configure itself. The name parameter is the name of the object in the context of that object, for example the name of a spout or bolt. The class can use that name however it sees fit. For example, it’s suggested that it be used when logging to identify the particular instance of a class if loadable object type might be used in multiple settings in the topology.
An alternative is to provide a builder for your module. The method is a static method in the form:

    public Object <method>(String name, DefaultingMap conf) {}

Similar to the constructor case, the builder is expected to create the appropriate module object and initialize it using the name and conf.  

### Supported Loadable Objects

TopoLoader uses loadable objects for a number of things:

#### Spouts
Spouts must be loadable objects that implement IRichSpout. The spout must configure itself from the conf parameter. TopoLoader handles adding the spout object to the topology.

#### Bolts

Similarly, bolts must be loadable objects that implement either IRichBolt or IBasicBolt. Again, each bolt must configure itself from the conf parameter, and TopoLoader handles adding the bolt to the topology.

#### Custom Groupings

If your topology makes use of CustomStreamGrouping, your grouping class must also be a loadable object.  Any parameters need to tune the grouping must come from the conf parameter.

####Topology Configuration

The ITopologyConfigure interface lets you make custom updates to the topology Config object that’s used when submitting the topology to storm. This interface is recognized for spouts, bolts, and for configuration components explicitly called out in the YAML. After loading a spout, bolt, or config object that implements ITopologyConfigure, TopoLoader will call:

    void doStormConfig(IToplogyConfigurator config){}

The config parameter gives the module access to name of the topology, and to the Config object that will be used when submitting the topology. doStormConfig() can update the Config as it sees fit. It’s up to you to deal with any conflicts that might result.

####User-Defined Loadable Objects

It’s entirely possible that your spouts, bolts, or other objects may need to use other loadable objects for their own use. In the Puffin topology, for example, we have a “lookup” bolt that needs to do lookups from several externally-defined tables. The location and format of the tables vary, though they all provide the same interface to the lookup bolt. In our topology, each lookup table is, itself, a loadable object. The lookup bolt loads each table from a subsection of the bolt’s configuration, and each table instance configures itself from its own table section.

You can load your own loadable object with the call:

    Object loadable = Loader.loadAndBuild(String name, DefaultingMap params);

(Yeah, I know that’s syntactically incorrect, but you get the idea.) This will construct the object with the expected constructor and return the result. It’s up to you to verify that the new object is the correct type.

## Topology YAML Definition

The topology YAML has three main sections, defining spouts, bolts, and custom configurations. The spout section is mandatory. (How can you have a topology without spouts?) The others are optional.

Each of the three has a top-level key: “spouts,” “bolts,” “config.” Under each section is a map, where the map key is the name of the component as it will be used in the topology, and the content is another map of component parameters.

Every loadable object in the YAML (spouts, bolts, and configs, as well as any custom loadables) must identify the class of the object to be created. There are two ways to do that:

 * Include a “class: &lt;classpath&gt;” item. The value is the class path of the java class for the loadable object. The class must have a constructor with arguments (String name, DefaultingMap conf). The constructor must configure itself from the conf contents.
 * Include a “builder: &lt;builder spec&gt;” item. The builder spec can be in the form &lt;classpath&gt;:&lt;method&gt;, or just &lt;classpath&gt;. If no &lt;method&gt; is provided, “builder” is assumed. The classpath and method identify a static method to call with arguments (String name, DefaultingMap conf). The method must instantiate and initialize the object (spout, bolt, or whatever) and return it.
Other TopoLoader-defined parameters are given below. Each loadable object can define any other required or optional settings it needs for its own use.

### Spout YAML Section

TopoLoader supports a “parallelism” setting for each spout. If it’s omitted, 1 is assumed. If it’s included and is &lt;= 0, the spout is disabled; it isn’t created, and isn’t added to the topology.
Also supported is a “spread: true|false” setting. Default is false. If this is set to true, the spout will be added to the TOPOLOGY_SPREAD_COMPONENTS list in Config, which in a multitenant environment asks storm to try to run the component’s workers on different nodes.

### Bolt YAML Section

Both parallelism and spread are supported for bolts, just as with spouts. In addition, TopoLoader looks for an “inputs” section that defines the inputs to this bolt. The inputs section is a list, each item in the list defines a source component and stream and how events are to be distributed to this bolt. In each case, if the stream name is omitted, “default” is assumed. Each element can be any one of:

    - <component>[:<stream>]
    - <component>:[<stream>]:<grouping>

Events from each upstream worker are distributed according to the given storm policy. &lt;grouping&gt; can be any of `shuffle`, `all`, `localOrShuffle`, `direct`, `none`, or `global`. If &lt;grouping&gt; is missing, `shuffle` is assumed.

    - <component>:[<stream>]:fields:<field>,<field>...

A standard field grouping based on the named fields.

    - class: <CustomStreamGrouping class path>
      component: <component name>
      [streamid: <stream name>]
      classSpecificParam: <whatever>

This defines a custom grouping. The named class must implement the CustomStreamGrouping interface. It will be loaded as a loadable object and used to set custom grouping for this input.

### Config YAML Section

The optional config YAML section is much like the spouts and bolts sections. It’s a map of individual custom configuration objects, where the key of each is a name (not used for anything except maybe logging) and parameters specific to that configuration object. Other than the “class” value in each map used to find the object, TopoLoader doesn’t use any other values in these maps.

###Topology Example

Here’s a very simple example of a topology YAML file:

    spouts:
      demospout:
        class: com.yahoo.demo.myspout
        parallelism: 10
        spoutsource: http:://example.com


    bolts:
      TransformBolt:
        builder: com.yahoo.demo.mybolt:demobuilder
        parallelism: 5
        inputs:
          - shuffle:demospout:data
          - all:dempsout:commands
        # This transform uses loadAndBuild() to 
        # instantiate a transformer.
        transformspec:
          class: com.yahoo.demo.demotransform
          transformSpec: “s/foo/bar/”


      DemoSink:
        class: com.yahoo.demo.mysink
        inputs:TransformBolt
        parallelism: 1
        outpath: /tmp/SpoutDump


    config:
      setusers:
        # Just an example, this could set the list of users
        # who are allowed to view this topology.
        class: com.yahoo.demo.setusers
        userlist: nikhil,tom,david,prateek
    
A pretty simple case, I realize, but you get the idea.

## Invoking TopoLoader

TopoLoader defines the main routine for your topology. You need to include TopoLoader in your jar. Then, you submit your topology with the command:

    storm jar <jarPath>  com.yahoo.storm.topology.TopoLoader [options] <pathToYaml>

Supported option parameters are:

    --dryrun               : Dryrun. Build topology but don't submit
    --help (-h)            : print help message
    --inactive (-i)        : Inactive. Submit topology but don't activate
    --local (-l) seconds   : run in local mode for this many seconds
    --maxparallel (-p) N   : topology max parallelism
    --name (-n) Name       : the name of the topology
    --overrides overrides  : comma-separated list of YAML files to update main YAML
    --workers (-w) N       : number of workers
    --debug                : print more verbose output
    --inactive             : Submit the topology but don’t activate it


## Advanced Features

### YAML Overrides

Sometimes it’s necessary to “tweak” your topology for a particular environment. Our Puffin topology, for example, couldn’t exercise the full production setup in our CI environment because some external dependencies simply weren’t available. Hence overrides. An override lets you specify one or more additional YAMLs that update or replace sections of your base topology definition. You probably don’t want to use overrides in your final, deployed configuration. But they’re really handy in test environments.

Each YAML is a hierarchy of Map<String,Object&gt;. Each map in an override YAML updates the corresponding map in the original YAML. For each element in an override map, basically the following rules are followed:

1. If the element value is null, the corresponding element in the YAML is removed. Otherwise...
2. If the element is not a submap, the value is added to the YAML, replacing any previous value with that key. Otherwise, for a submap…
3. If the submap contains “deleteSection: true”, the corresponding section in the YAML is removed.
4. If the submap contains “replaceSection: true”, the section from the override replaces the entire YAML subsection.
5. If there is no corresponding submap in the YAML, the override submap is inserted into the YAML.
6. If none of the above, the YAML submap is updated recursively from the override submap.

### List Patch

The Puffin topology YAML defines a number of lists of fields that control the list of dimension and metric fields that are passed between components in the topology. It was handy to be able to tweak those lists for use in the project’s smoke test. (We added a field in smoke test to facility data validation, even though those fields weren’t used in production.)

List patch lets you add or remove items from a string list declared elsewhere in the YAML. This may seem like an odd ability and it’s unlikely it will ever be used in a production topology. Its main use is in an override that Puffin used by the smoke test.

Here’s an example of a list patch section from an override YAML:

    listpatch:
        bolts>AdwSink>dimensions:
            add:
               - event_uuid
            remove:
               - line_id
        spouts>PBPSpout>schema:
            add:
               - event_uuid

Note that the key for each entry under listpatch is essentially the path (elements delimited by ‘&gt;’) to the item to be patched. So, for example, “bolts&gt;AdwSink&gt;dimensions” means we’ll patch the “dimensions” list within “AdwSink” in the “bolts” main section. The path can be arbitrarily deep.

Every element up to the last must exist. If the last element doesn’t exist, a new, initially empty, list will be created.
Under each of the listpatch paths are one or two elements. “add” is a list of strings to add to the identified list, and “remove” is a list of strings to remove.

So, in the above example, “event_uuid” would be added to bolts&gt;AdwSink&gt;dimensions and “line_id” removed if it was there.
