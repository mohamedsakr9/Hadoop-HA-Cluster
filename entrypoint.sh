#!/bin/bash

echo "$(date) Sourcing environment variables..."
# Manually set environment variables to avoid sourcing issues
export HADOOP_HOME=/home/huser/Data/hadoop-3.3.6
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export ZOOKEEPER_HOME=/home/huser/zookeeper-3.5.9
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$ZOOKEEPER_HOME/bin

NAMENODE_DIR=/home/huser/Data/hadoop-3.3.6/namenode
DATANODE_DIR=/home/huser/Data/hadoop-3.3.6/datanode
ZK_DIR=/home/huser/Data/zookeeper

echo "$(date) Starting SSH service..."
service ssh start

# Set up ZooKeeper myid
su - huser
echo "$(date) Configuring ZooKeeper myid and logs..."
mkdir -p $ZK_DIR/logs

case "$(hostname)" in
    "m1")
        echo "1" > $ZK_DIR/myid
        ;;
    "m2")
        echo "2" > $ZK_DIR/myid
        ;;
    "m3")
        echo "3" > $ZK_DIR/myid
        ;;
    "w"*)
        # For worker nodes, set myid based on NODE_ID
        echo "$NODE_ID" > $ZK_DIR/myid
        echo "Setting worker node ZK ID to $NODE_ID"
        ;;
    *)
        echo "Unknown host. Skipping myid setup."
        ;;
esac



# Create DataNode directory with correct permissions
mkdir -p $DATANODE_DIR

# Run all services as the huser user
exec gosu huser bash -c "

# For worker nodes (hostname starts with 'w')
if [[ \"\$(hostname)\" =~ ^w.* ]]; then
    
    echo \"\$(date) Starting DataNode on worker node \$(hostname)...\"
    hdfs --daemon start datanode
    echo \"\$(date) Starting NodeManager on worker node \$(hostname)...\"
    yarn --daemon start nodemanager
    
    echo \"\$(date) Worker node services started successfully! Monitoring logs...\"
    su huser
    tail -f /dev/null
    exit 0
fi

# For master nodes (continuing with existing script)
echo \"\$(date) Starting JournalNode on \$(hostname)...\"
hdfs --daemon start journalnode
su huser
# Format NameNode and ZooKeeper Failover Controller (ZKFC) only if needed
if [ \"\$(hostname)\" == \"m1\" ]; then
    # Format NameNode if not formatted
    if [ ! -d \"$NAMENODE_DIR/current\" ]; then
        echo \"\$(date) Formatting NameNode as it hasn't been formatted yet...\"
        hdfs namenode -format -force
    else
        echo \"\$(date) NameNode is already formatted. Skipping formatting...\"
    fi

    # Start NameNode and ZooKeeper
    echo \"\$(date) Starting ZooKeeper on m1...\"
    zkServer.sh start
    
    echo \"\$(date) Starting NameNode on m1...\"
    hdfs --daemon start namenode

    # Format ZKFC if not formatted - using the specified ZooKeeper check
    if echo \"ls /hadoop-ha\" | zkCli.sh -server m1:2181 | grep -q sakrcluster; then
        echo \"\$(date) ZKFC is already formatted. Skipping formatting...\"
    else
        echo \"\$(date) Formatting Zookeeper Failover Controller (ZKFC) as it hasn't been formatted yet...\"
        hdfs zkfc -formatZK -force
    fi

    echo \"\$(date) Starting ZKFC on m1...\"
    hdfs --daemon start zkfc
    
    # Create YARN HA ZooKeeper paths if they don't exist
    echo \"\$(date) Setting up YARN HA ZooKeeper paths...\"
    
    # Check if /rmstore exists
    if echo \"ls /rmstore\" | zkCli.sh -server m1:2181 2>&1 | grep -q \"Node does not exist\"; then
        echo \"\$(date) Creating /rmstore ZooKeeper path for YARN HA...\"
        echo \"create /rmstore\" | zkCli.sh -server m1:2181
    else
        echo \"\$(date) /rmstore ZooKeeper path already exists\"
    fi
    
    # Check if /yarn-leader-election exists
    if echo \"ls /yarn-leader-election\" | zkCli.sh -server m1:2181 2>&1 | grep -q \"Node does not exist\"; then
        echo \"\$(date) Creating /yarn-leader-election ZooKeeper path for YARN HA...\"
        echo \"create /yarn-leader-election\" | zkCli.sh -server m1:2181
    else
        echo \"\$(date) /yarn-leader-election ZooKeeper path already exists\"
    fi

elif [ \"\$(hostname)\" == \"m2\" ] || [ \"\$(hostname)\" == \"m3\" ]; then
    # Start ZooKeeper first on all nodes
    echo \"\$(date) Starting ZooKeeper on \$(hostname)...\"
    zkServer.sh start
    
    # Wait for ZooKeeper to fully start
    sleep 5
    
    # Bootstrap Standby for m2 and m3
    if [ ! -d \"$NAMENODE_DIR/current\" ]; then
        echo \"\$(date) Bootstrapping Standby on \$(hostname)...\"
        hdfs namenode -bootstrapStandby
    else
        echo \"\$(date) Standby NameNode already bootstrapped. Skipping...\"
    fi

    echo \"\$(date) Starting NameNode on \$(hostname)...\"
    hdfs --daemon start namenode

    echo \"\$(date) Starting ZKFC on \$(hostname)...\"
    hdfs --daemon start zkfc
fi

# Wait for HDFS services to fully start
sleep 5

# Start ResourceManager after ZooKeeper paths are set up
echo \"\$(date) Starting ResourceManager on \$(hostname)...\"
yarn --daemon start resourcemanager

echo \"\$(date) All services started successfully! Monitoring logs...\"
tail -f /dev/null
"
