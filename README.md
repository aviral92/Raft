Raft
======================================================================================================================================
This repository is a Golanf implementation of the Raft consensus protocol. Raft is based on "[Raft: In Search of an Understandable Consensus Algorithm](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf)".

Protocol Description
---------------------
Raft nodes are always in one of three states: follower, candidate or leader. All nodes initially start out as a follower. In this state, nodes can accept log entries from a leader and cast votes. If no entries are received for some time, nodes self-promote to the candidate state. In the candidate state nodes request votes from their peers. If a candidate receives a quorum of votes, then it is promoted to a leader. The leader must accept new log entries and replicate to all the other followers. In addition, if stale reads are not acceptable, all queries must also be performed on the leader.

Once a cluster has a leader, it is able to accept new log entries. A client can request that a leader append a new log entry, which is an opaque binary blob to Raft. The leader then writes the entry to durable storage and attempts to replicate to a quorum of followers. Once the log entry is considered committed, it can be applied to a finite state machine. The finite state machine is application specific, and is implemented using an interface.

An obvious question relates to the unbounded nature of a replicated log. Raft provides a mechanism by which the current state is snapshotted, and the log is compacted. Because of the FSM abstraction, restoring the state of the FSM must result in the same state as a replay of old logs. This allows Raft to capture the FSM state at a point in time, and then remove all the logs that were used to reach that state. This is performed automatically without user intervention, and prevents unbounded disk usage as well as minimizing time spent replaying logs.

Lastly, there is the issue of updating the peer set when new servers are joining or existing servers are leaving. As long as a quorum of nodes is available, this is not an issue as Raft provides mechanisms to dynamically update the peer set. If a quorum of nodes is unavailable, then this becomes a very challenging issue. For example, suppose there are only 2 peers, A and B. The quorum size is also 2, meaning both nodes must agree to commit a log entry. If either A or B fails, it is now impossible to reach quorum. This means the cluster is unable to add, or remove a node, or commit any additional log entries. This results in unavailability. At this point, manual intervention would be required to remove either A or B, and to restart the remaining node in bootstrap mode.

A Raft cluster of 3 nodes can tolerate a single node failure, while a cluster of 5 can tolerate 2 node failures. The recommended configuration is to either run 3 or 5 raft servers. This maximizes availability without greatly sacrificing performance.

In terms of performance, Raft is comparable to Paxos. Assuming stable leadership, committing a log entry requires a single round trip to half of the cluster. Thus performance is bound by disk I/O and network latency.
The code itself is in `server`. `client` contains a rather trivial client designed to test lab0. For testing you can use
Kubernetes, we have provided a script in `launch-tool/launch.py`. Please not that `launch.py` hardcodes a bunch of
assumptions about how pods are created, about the fact that we are running under minikube, and that the image itself is
named `local/raft-peer`. As such one can adopt this script for other purposes, but this will need some work.

Code Structure
---------------------------------------------------------------------------------------------------------------------------------------
-   main.go is responsible for argument parsing, create the client gRPC server and call the serve function which acts as the main control loop of our application. You might want to look at the code in this file to see how you can make the random number generator deterministic (by passing in -seed followed by a positive integer), how you can select the ports used by the program (which might come in handy to allow you to test with Kubernetes) and how you can pass in a set of peers.
-   peers.go is just a simple set of utilities that helps with argument parsing for peers.
-   kvstore.go is most of the code from Lab 0, with some minor extensions. The code for the key-value store lives here.
-   serve.go is where the control loop for this program lives, and where much of your Raft implementation is likely to live.

As described above the serve function in serve.go acts as the main state machine for your implementation. This function is is responsible for processing client requests, timeouts, Raft requests and some internal communication. The main challenge in building such a function is that GRPC uses a different thread to handle each connection (from clients or from other Raft servers), timers in Go run in their own Go routines, etc. Additionally, we would ideally like the state machine to continue making progress even when RPC calls (e.g., to other peers) hang due to network failures or delays, and as a result we will be running these from their own Go routine (see here for an example of this in action). However, in order to build a state machine we need to handle all of these events from one thread, and we rely on a set of channels to do so. The figure above shows how data flows between the state machine loop and all of these threads, and each arrow is tagged with the channel it uses for communication. 

How to use this implementation
-------------------------------
To use Kubernetes with this project use `./create-docker-image.sh` to first create a Docker image. Then:

-   `./launch.py boot <num peers>` will boot a cluster with `num peers` participants. Each participant is given a list of
  all other participants (so you can connect to them).
-   `./launch.py list` lists all peers in the current cluster. You can use `kubectl logs <name>` to access the log for a
    particular pod.
-   `./launch.py kill <n>` can be used to kill the nth pod.
-   `./launch.py launch <n>` can be used to relaunch the nth pod (e.g., after it is killed).
-   `./launch.py shutdown` will kill all pods, shutting down the cluster.
-   `./launch.py client-url <n>` can be used to get the URL for the nth pod. One example use of this is `./client
    $(../launch-tool/launch.py client-url 1)` to get a client to connect to pod 1.

Future work 
----------------------------------------------------------------------------------------------------------------------------------------
-   Log compaction:  Implement snapshots and the InstallSnapshot RPC described in Section 7 of the paper.
-   Cluster membership changes: We assume that all peers will be known at boot, and we will not partition the cluster into independent quorums. TODO: implement the mechanisms in Section 6. However, the current impementation handle cases where one or more servers are killed, and started again.
