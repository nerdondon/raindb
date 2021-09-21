# RainDB

A persistent key-value store implemented based on an LSM tree implemented in Rust

## Motivation

This project serves more as a toy project and tool for learning about database internals. As such,
the code will be annotated as much as possible.

## High Level Design

The KV store is backed by an LSM tree and has/will have the traditional components that come from
the fact:

1. Memtable - Initially a skip list but will be swappable in the future with other data structures
1. SSTable (sorted string tables) - A simpler implementation at first e.g. no checksumming
1. Write ahead log
1. Bloom filter

Other components include a buffer pool manager and an API server implemented with gRPC.

## Future ideas

- Use as part of a distributed store with Raft for leader election
  - Add data sharding

## References

- More notes and references can be found on [my blog](https://blog.nerdondon.com)
- Database Internals by Alex Petrov
