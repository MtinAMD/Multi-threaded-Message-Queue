# Multi-threaded Message Queue

## Overview
This project implements a simplified **multi-threaded message queue system** in Java, inspired by real-world message brokers like **Kafka** or **RabbitMQ**.  
The system demonstrates key operating system and concurrency concepts: **threads**, **producer–consumer synchronization**, and **thread-safe data structures**.

## Features
- **Broker**: Manages the entire system and threads.
- **Topics**: Independent queues where messages are stored.
- **Producers**: Multiple threads generating tasks with attributes:
  - `priority` (0–4)
  - `duration` (simulated processing time)
  - `TTL` (time-to-live)
- **Consumers (Workers)**: Multiple threads that fetch and process messages.
- **Garbage Collector**: A background thread periodically removes expired messages (based on TTL).
- **Dashboard (TUI)**: Prints live information every few seconds:
  - Queue sizes and head-of-queue info
  - Distribution of tasks per topic and per consumer
  - Average processing time per priority


- **Priority-aware scheduling**: High-priority tasks are served first, then shorter duration, then FIFO.
- **Bounded buffer**: Configurable maximum capacity per topic; producers block if full.
- To prevent race conditions, a `ReentrantLock` was used along with two `Condition` objects:
  - `notEmpty`: To prevent consumption when the queue is empty.
  - `notFull`: To prevent production when the queue is full.
- Each topic has an independent lock so that consumers and producers can work on different topics simultaneously. Statistical data was stored using `ConcurrentHashMap` and `AtomicLong` to be updated safely without heavy locking.
- **Deleting Expired (TTL) Messages** 
This is handled at two levels:
  - Consumers check for and remove expired messages at the time of dequeuing.
  - The GC thread periodically cleans expired messages from the queues.

## Build & Run
1. Compile & run:
   ```bash
   javac message_broker.java
   java message_broker

- The program will run for a fixed time (default: 15 seconds). To run indefinitely, set:
  - static final long RUN_SECONDS = 0;
