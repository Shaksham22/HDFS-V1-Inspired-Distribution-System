# HDFS-V1-Inspired-Distribution-System

## Overview

This project implements a **distributed file system inspired by the first-generation Hadoop Distributed File System (HDFS v1)**. It follows a **NameNode–DataNode architecture**, supports block-based storage, replication, and client-driven access, and communicates entirely using **TCP sockets**.

The system is designed to run across **multiple machines on the same network**, with no shared filesystem assumptions, demonstrating how large-scale storage systems coordinate metadata, distribute data, and maintain availability through replication.

This project was developed as part of a **Big Data course**, with the goal of going beyond typical analytics-focused assignments and instead building a **storage-layer system** to understand the internal architecture of big data platforms.

---

## Architecture Overview

The system is composed of three primary components:

### NameNode
- Acts as the **central metadata manager**
- Maintains:
  - file → block mappings
  - block → DataNode replica locations
- Assigns:
  - unique block IDs
  - DataNode IDs
- Performs **capacity-aware block placement** using DataNode heartbeat information
- Coordinates communication between clients and DataNodes

### DataNodes
- Store file blocks on local disk
- Periodically send **heartbeats** with disk usage and availability
- Support **pipeline replication**, forwarding blocks to other DataNodes to satisfy the replication factor
- Serve block read and local compute requests, demonstrating **data locality**

### Client
- Splits large files into fixed-size blocks (target size ≈ 128 MB)
- Requests block placement decisions from the NameNode
- Transfers data directly to DataNodes
- Reconstructs files using metadata provided by the NameNode

---

## Distributed Design

- All components communicate using **TCP sockets**
- No reliance on shared storage or localhost-only assumptions
- NameNode, DataNodes, and Client can run on **different physical machines**
- Node addresses and ports are exchanged dynamically at runtime
- Supports multiple DataNodes to simulate a real cluster environment

This design allows the system to function as a **true distributed application**, not just a single-host simulation.

---

## Block Storage and Read Semantics

- Files are split into fixed-size blocks for storage efficiency
- Block size is treated as a **target**, not a strict read boundary
- Read and compute operations prioritize:
  - independent readability
  - record correctness
- This mirrors real HDFS behavior, where storage blocks are fixed-size but compute-level reads are record-aware

---

## Replication and Fault Awareness

- Configurable replication factor
- Blocks are replicated across multiple DataNodes
- Pipeline-style replication minimizes redundant client uploads
- Heartbeat-based monitoring allows the NameNode to:
  - track DataNode availability
  - make informed block placement decisions
- The architecture supports maintaining data availability through replicas in the presence of node failures

---

## MapReduce-Inspired Local Computation

- DataNodes can execute **local map-style computations** on stored blocks
- Demonstrates the principle of **moving computation to data**
- Illustrates how distributed storage enables scalable parallel processing
- While not a full MapReduce or YARN implementation, the design reflects the same locality-driven execution model used in Hadoop

---

## Key Features

- HDFS v1–style NameNode/DataNode architecture  
- Block-based storage with replication  
- Heartbeat-based DataNode monitoring  
- Capacity-aware block placement  
- Pipeline replication between DataNodes  
- Socket-based communication across multiple machines  
- Support for large files and distributed execution  

---

## Motivation

Instead of building analytics on top of existing big data frameworks, this project focuses on the **infrastructure layer**—specifically how distributed storage systems are designed and coordinated internally.

It reflects an interest in:
- Distributed systems architecture
- Big data storage internals
- Fault tolerance and replication
- Data locality and scalability

---

## Disclaimer

This system is **HDFS-inspired** and is not a replacement for Hadoop HDFS.  
Advanced production features such as high-availability NameNodes, persistent metadata logging, and full job scheduling frameworks are intentionally out of scope.

---
