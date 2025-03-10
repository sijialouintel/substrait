---
title: Home
---
# Substrait: Cross-Language Serialization for Relational Algebra





## Project Vision

Create a well-defined, cross-language [specification](spec/specification) for data compute operations. This includes a declaration of common operations, custom operations and one or more serialized representations of this specification. The spec focuses on the semantics of each operation and a consistent way to describe.

In many ways, the goal of this project is similar to that of the Apache Arrow project. Arrow is focused on a standardized memory representation of columnar data. Substrait is focused on what should be done to data.



## Example Use Cases

* Communicate a compute plan between a SQL parser and an execution engine (e.g. Calcite SQL parsing to Arrow C++ compute kernel)
* Serialize a plan that represents a SQL view for consistent use in multiple systems (e.g. Iceberg views in Spark and Trino)
* Create an alternative plan generation implementation that can connect an existing end-user compute expression system to an existing end-user processing engine (e.g. Pandas operations executed inside SingleStore)
* Build a pluggable plan visualization tool (e.g. D3 based plan visualizer)



## Community Principles

* Be inclusive and open to all. If you want to join the project, open a PR or [issue](https://github.com/substrait-io/substrait/issues), start a [discussion](https://github.com/substrait-io/substrait/discussions) or [join the Slack Channel](https://join.slack.com/t/substrait/shared_invite/zt-vivbux2c-~B1jEWcR0wYhq5k4LHuoLQ).
* Ensure a diverse set of contributors that come from multiple data backgrounds to maximize general utility.
* Build a specification based on open consensus.
* Avoid over-reliance/coupling to any single technology.
* Make the specification and all tools freely available on a permissive license (ApacheV2)



## Related Technologies

* [Apache Calcite](https://calcite.apache.org/): Many ideas in Substrait are inspired by the Calcite project. Calcite is a great JVM-based sql query parsing and optimization framework. A key goal of the Substrait project is to expose Calcite capabilities more easily to non-jvm technologies as well as expose query planning operations as microservices.
* [Apache Arrow](https://arrow.apache.org/): The Arrow format for data is what the Substrait specification attempts to be for compute expressions. A key goal of Substrait is to enable substrait producers to execute work within the Arrow Rust and C++ compute kernels.



## Why not just do this within an existing OSS project?

A key goal of the Substrait project is to not be coupled to any single existing technology. Trying to get people involved in something can be difficult when it seems to be primarily driven by the opinions and habits of a single community. In many ways, this situation is similar to the early situation with Arrow. The precursor to Arrow was the Apache Drill ValueVectors concepts. As part of creating Arrow, [Wes](https://www.linkedin.com/in/wesmckinn/) and [Jacques](https://www.linkedin.com/in/jacquesnadeau/) recognized the need to create a new community to build a fresh consensus (beyond just what the Apache Drill community wanted). This separation and new independent community was a key ingredient to Arrow's current success. The needs here are much the same--many separate communities could benefit from Substrait but each have their own pain points, type systems, development processes and timelines. To help resolve these tensions, one of the approaches proposed in Substrait is to set a bar that at least two of the top four OSS data technologies (Arrow, Spark, Iceberg, Trino) supports something before incorporating it directly into the Substrait specification. (Another goal is to support strong extension points at key locations to avoid this bar being a limiter to broad adoption.)



## Why the name Substrait?

A strait is a narrow connector of water between two other pieces of water. In analytics, data is often thought of as water. Substrait is focused on instructions related to the data. In other words, what defines or supports the movement of water between one or more larger systems. Thus the underlayment for the strait connecting different pools of water => sub-strait.
