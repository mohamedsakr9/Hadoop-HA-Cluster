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

# Make sure necessary directories exist
mkdir -p $ZK_DIR
mkdir -p $NAMENODE_DIR
mkdir -p $DATANODE_DIR
mkdir -p $ZK_DIR/logs

echo "$(date) Starting SSH service..."
# Create /run/sshd directory if it doesn't exist
mkdir -p /run/sshd
# Start SSH service
/usr/sbin/sshd

echo "$(date) Configuring ZooKeeper myid..."
# Set up ZooKeeper myid based on hostname
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
        # For worker nodes, set myid based on NODE_ID or default to hostname number
        NODE_NUM=$(echo "$(hostname)" | sed 's/w//')
        ID=${NODE_ID:-$NODE_NUM}
        echo "$ID" > $ZK_DIR/myid
        echo "Setting worker node ZK ID to $ID"
        ;;
    *)
        echo "Unknown host. Using default myid=1"
        echo "1" > $ZK_DIR/myid
        ;;
esac

# Make sure myid has correct ownership
chown huser:hadoop $ZK_DIR/myid

# Run all services as the huser user
echo "$(date) Switching to huser user to run services..."
exec gosu huser bash -c "
echo \"Running as \$(whoami) on \$(hostname)\"

# For worker nodes (hostname starts with 'w')
if [[ \"\$(hostname)\" =~ ^w.* ]]; then
    echo \"\$(date) Starting DataNode on worker node \$(hostname)...\"
    hdfs --daemon start datanode
    
    echo \"\$(date) Starting NodeManager on worker node \$(hostname)...\"
    yarn --daemon start nodemanager
    
    echo \"\$(date) Worker node services started successfully! Monitoring logs...\"
    tail -f /dev/null
    exit 0
fi

# For master nodes
echo \"\$(date) Starting JournalNode on \$(hostname)...\"
hdfs --daemon start journalnode

# Give JournalNode time to start
sleep 3

# Start ZooKeeper on all master nodes
echo \"\$(date) Starting ZooKeeper on \$(hostname)...\"
$ZOOKEEPER_HOME/bin/zkServer.sh start

# Give ZooKeeper time to start
sleep 5

# Master node 1 (m1) specific operations
if [ \"\$(hostname)\" == \"m1\" ]; then
    # Format NameNode if not formatted
    if [ ! -d \"$NAMENODE_DIR/current\" ]; then
        echo \"\$(date) Formatting NameNode as it hasn't been formatted yet...\"
        hdfs namenode -format -force
    else
        echo \"\$(date) NameNode is already formatted. Skipping formatting...\"
    fi
    
    echo \"\$(date) Starting NameNode on m1...\"
    hdfs --daemon start namenode
    
    # Wait for NameNode to start
    sleep 5
    
    # Format ZKFC if not formatted
    echo \"\$(date) Attempting to check if ZKFC is already formatted...\"
    if $ZOOKEEPER_HOME/bin/zkCli.sh -server localhost:2181 ls /hadoop-ha 2>&1 | grep -q 'sakrcluster'; then
        echo \"\$(date) ZKFC is already formatted. Skipping formatting...\"
    else
        echo \"\$(date) Formatting Zookeeper Failover Controller (ZKFC)...\"
        hdfs zkfc -formatZK -force
    fi
    
    echo \"\$(date) Starting ZKFC on m1...\"
    hdfs --daemon start zkfc
    
    # Create YARN HA ZooKeeper paths
    echo \"\$(date) Setting up YARN HA ZooKeeper paths...\"
    
    # Try to create /rmstore
    $ZOOKEEPER_HOME/bin/zkCli.sh -ser
