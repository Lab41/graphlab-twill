Graphlab-Twill
==============
[![Build Status](https://travis-ci.org/Lab41/graphlab-twill.png?branch=master)](https://travis-ci.org/Lab41/graphlab-twill) [![Coverage Status](https://coveralls.io/repos/Lab41/graphlab-twill/badge.png)](https://coveralls.io/r/Lab41/graphlab-twill)

Graphlab-Twill is a Graphlab scheduler for YARN.

Install Instructions
====================

1. Check out the code. As of right now, we have to use the Lab41 fork of Apache Twill.

```
% git clone https://github.com/Lab41/incubator-twill.git
% cd incubator-twill
% git checkout sync
% mvn install -DskipTests=true
% cd ..
% git clone https://github.com/Lab41/graphlab-twill.git
% cd graphlab-twill
% mvn package
```

2. Run the example.

```
% java \
    -cp "target/graphlab-twill-1.0-SNAPSHOT.jar:`hadoop cpasspath`"
    org.lab41.graphlab.twill.Main \
    --instances 5 \
    $ZOOKEEPER_URL \
    sh -c '
    export HADOOP_CONF_DIR=/etc/hadoop/conf;
    export CLASSPATH=$(
        for i in `hadoop classpath | sed "s/:/ /g"`; do
        echo $i
    done | xargs | sed "s/ /:/g"
    );
    /path/to/graphlab/pagerank/on/cluster \
    --graph hdfs://$HDFS_URL/graph.tsv \
    --format tsv \
    --saveprefix hdfs://$HDFS_URL/graph_out
    '
```

Required Dependencies
---------------------

- Java
- Maven
- Zookeeper
- Hadoop YARN

Related repositories
====================

 - [Dendrite](https://github.com/Lab41/Dendrite)

Contributing to Graphlab-Twill
==============================

What to contribute?  Awesome!  Issue a pull request or see more details [here](https://github.com/Lab41/graphlab-twill/blob/master/CONTRIBUTING.md).
