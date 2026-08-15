# Streaming for the AI age

## Introduction

Flink is the default tool for real-time data processing at scale. It has massive adoption across
the globe, contains source and sink integrations for various data systems, and boasts a fantastic
developer community made up of engineers at every big tech company. Flink has already cemented its
own importance in the AI age, in which data publishing continues to scale exponentially.

Flink is showing its age. It became an Apache project in 2014, not long after Apache Spark. Both of
these technologies are still dominant in their domains - Spark for batch processing, and Flink for
stream processing. Their ubiquity, community, production experience, and integration with other
data tooling make it hard to justify moving away from them. Yet despite all of these great
qualities, their performance leaves much to be desired.

In 2022, Databricks released "Photon." Photon was Databricks' own attempt at keeping all of Spark's
aforementioned benefits (and API/identical results), while improving its performance. To do so,
they rewrote the execution engine using vectorized C++ query operators.

Around the same time period, we've seen many more Spark accelerators emerge, many of them open
source. Some examples are Project Gluten (Spark with Meta Velox execution), Nvidia Rapids (Spark
with GPU execution), and DataFusion Comet (Spark with DataFusion execution) - in addition to some
closed-source ones out of Google and Microsoft. Generally speaking, the designs of these projects
all share a similar architecture. They reuse the distributed computing, query planning, and job
scheduling from Spark, and they improve the layer that actually touches the individual data
entries.

Surprisingly, the same trend is less common in data streaming - there are no such popular
open-source systems to accelerate Flink. To be fair, I have seen closed-source engine overhauls
from Alibaba (Flash) and Ververica (Vera-X, owned by Alibaba, basically the same project). They
claim between 5x-10x performance improvements over normal Flink, yet the project is behind a
paywall. There have been some initial efforts to speed up Flink within Project Gluten (OSS), but no
substantial progress.

Is Flink really "acceleratable"? After all, streaming data generally updates one row at a time and
data often comes in and out via Kafka (row-based). In my experience thus far, the benefits of
cutting out the JVM and operating on column batches speak for themselves (and if I can't convince
you, hopefully Alibaba's work does). I look forward to outlining them in detail in this blog
series.

You may be under the impression that all Flink jobs are defined with handwritten Java code (the
DataStream API) and therefore impossible to accelerate. While that's definitely a popular way of
running Flink jobs, the SQL/table API has gained a lot of popularity in recent years. Confluent and
AWS are pushing for more managed Flink SQL jobs. I spoke to a different big American tech company,
who informed me that out of a few thousand jobs, a third are written in SQL. Newer streaming
systems like RisingWave and Arroyo are generally built around SQL as a first-class citizen. Flink
SQL itself is very flexible, and adds a lot of new keywords that expose streaming primitives
(certain windowing concepts, exposing watermarks, processing time, even UDFs). Given the limited
number of predefined operators, we can write our own Flink-compatible implementations of them to
improve their performance.

Why am I doing this, and why is this the right time to do it? I'm no Flink expert and I'm certainly
no god-tier systems programmer. Even though I may not have an edge in raw engineering talent, I
strive to have it in community-building. I've spent the last four years publishing YouTube videos
on distributed systems, systems design, and data, and hope that I can get engineers excited about
contributing to this project. AI can help you produce way more than just a few years ago (not
always a good thing), but I find it to be a lot less effective in building trust in a way that
uniquely requires human-to-human interaction. There's more data now than there's ever been, and yet
hardware is harder to come by now than it has ever been.

## High Level Approach

What do Flash, Gluten, and Comet have in common? They all process data in column batches. What does
that really mean? In traditional data systems, whether batch or streaming, we'll handle a single
row at a time in a for loop, executing any operations that we need to perform on it. That could be
a projection, a filter, or even joining it to another dataset.

In relatively recent history, the CPU has become the main querying bottleneck (as opposed to
network or disk I/O). We need to take any gains that we can. One common way of doing so is to
process chunks of data as "vectors," and perform an operation on a column at a time, rather than a
row at a time.

Why is this so helpful? For one, performing the same operation over and over again allows us to get
better CPU cache locality (due to prefetching contiguous memory bytes) and iterate in tight loops.
Additionally, doing the same simple operation on the same data type multiple times in a row allows
us to take more advantage of CPU SIMD instructions, achieving parallelism within a single thread.

So we have to represent all of our data in memory differently now? What a pain in the butt! Well,
actually, there are libraries that have made standardized formats for representing tabular data in
memory in a column-oriented fashion. One example is the Apache Arrow project - which has libraries
to work with this representation in Java, Rust, C++, Go, Python, and more. Arrow is a well-built-out
project with tons of support from existing query engines, making it a natural building block for
StreamFusion. On top of that, we can use Apache DataFusion as the main workhorse for the execution
layer. DataFusion is a Rust library (meaning no JVM and garbage collection, but rather fine-grained
control over memory allocations) which is highly optimized for performing standard SQL query
operations. It's built in a modular fashion, allowing us to strip out only the parts that we need
(compute primitives) and avoid the parts that we may not (SQL parser and optimizer). DataFusion
also treats Arrow buffers as first-class citizens, allowing them to be zero-copy inputs and outputs
of its computations.

However, DataFusion itself focuses on batch processing, not streaming. This means that while
DataFusion is instrumental in speeding up certain projections, filters, (windowed)
joins/aggregations, etc., it cannot cover the whole spectrum of Flink SQL operators. For this, we
need to write our own optimized code. Fortunately, a few other OSS streaming engines already exist
to use as inspiration. RisingWave and Arroyo are both popular Rust-based streaming engines that
have popped up in the past few years which implemented columnar streaming systems from scratch.
Besides implementing all of our execution layers using native code, we still want to keep
everything else managed by Flink's existing battle-tested software.

StreamFusion operators still hook into Flink's checkpointing, autoscaling, planning, and monitoring
mechanisms. And so our journey begins.

## Planning Layer

In StreamFusion, like the Spark accelerators I've referenced, we allow Flink to establish the plan
for a particular job so that we can then look through that job plan and determine which of the
Flink SQL operators we can convert to native execution. Unlike some of their approaches, I've
established a very simple heuristic, which I'll call "all-or-nothing" acceleration. Either the
entire plan is "accelerated," or it isn't.

The way we can determine whether a plan is acceleratable is by looking at the middle nodes
(everything except for the source and sink). If each one can be accelerated, the whole plan will
be - otherwise it will not. We gate acceleration on an individual plan node on two criteria. The
first is whether we've actually implemented a native version of it. Some operators are somewhat
obscure and I haven't bothered to build them yet without a use case. Other reasons for "falling
back" are operators that are hard to reimplement for correctness reasons. A good example would be
regex evaluation, which may return subtly different values between Java and Rust. For such
operations, we may expose a toggle to allow a user to accelerate them at the cost of non-identical
results. Achieving exact parity to Flink results is our north star.

Our "middle" nodes only communicate with one another using Arrow batches - each one takes them in
and spits them out. For some sources/sinks (e.g. a Parquet file), data is already stored/produced in
a columnar fashion and we pay barely any performance penalty to transform it to Arrow. For others,
we need to transpose the data from rows to column batches or vice versa - making this operation
efficient is paramount for good performance.

## Stateful Operators

Dealing with state has been, unsurprisingly, non-trivial. State management and checkpointing is
Flink's biggest value-add and we need to be sure that our stream processing accelerator hooks into
it as well. However, we don't want to have to reimplement Flink's asynchronous barrier snapshotting
algorithm in Rust. Instead, we let checkpoints and watermarks flow through our job graph in the JVM
like they normally would. When they reach a native operator (which has both a Java hook and Rust
logic which gets invoked over JNI), the native operator's checkpointing/snapshot logic gets
triggered.

Different stream processing systems may use different checkpointing algorithms (for example,
Feldera is based on DBSP) which makes their operators non-trivial to slot into our systems.
Fortunately for us, both Arroyo and RisingWave (and TimePlus Proton, which is written in C++) are
based on the Chandy-Lamport-style barrier-passing mechanism used in Flink - meaning that we can
easily port their code into StreamFusion.

StreamFusion stateful operators use (as needed) keyed "raw state." This means that the operator
state bytes are opaque to Java, but are still associated with a statically defined key group at job
creation time (key groups are based on the job's max parallelism) which Flink knows how to
interpret. This way, as the job scales, Flink can redistribute the proper key groups to each
parallel invocation of a task, without necessarily being able to interpret the state internals
itself (it just sees an opaque blob). This concept works for both aligned and unaligned checkpoints.

In order to get this working properly, it's important that we can actually exactly replicate
Flink's hashing logic so that incoming rows are routed to the proper task based on their key. Flink
determines key routing based on the exact bytes of the row key itself, meaning that we had to employ
a Rust-based representation of Flink's internal binary row layout. Fortunately, some Rust logic
already exists for this (Paimon and Fluss Rust clients, both part of the Flink ecosystem) and
StreamFusion borrows it.

## Custom Connectors

The majority of our data processing in StreamFusion happens on top of Arrow buffers, which are
columnar. This means that when reading data from a source function and submitting data to a sink
function, we want to do the minimal amount of data conversions. Flink, by default, operates on
`RowData` (an internal Flink representation of a row of data). Every source must convert its output
to `RowData` and every sink must take `RowData` as input. However, converting all source bytes to
`RowData` just to convert them to Arrow batches is wasteful, and the same logic applies in a sink.

To optimize the edges of our pipeline we've begun to write custom connectors. One example worth
mentioning is Kafka, which is a typical source for many Flink jobs. Kafka data might be stored as
(Confluent) Avro, JSON, or Protobuf. To get the best performance here, we want to skip the
conversion from bytes on the broker to `RowData`, and instead decode it straight into an Arrow
buffer. To that end, we've benchmarked a variety of different approaches and libraries for each
format - taking advantage of SIMD decoding implementations where possible (`simd-json`,
`arrow-avro`).

For now, the broker is still polled from and written to via the Java client (just serialization and
deserialization are native) to maximize Flink Kafka compatibility and code simplicity. In the
future, we can reap more performance by pushing the API calls themselves down to the native layer.

In release 0.1.0, we only boast custom Kafka connectors, but in order to get the maximum performance
benefit from StreamFusion I'll soon begin iterating on file/table format connectors, Fluss, and
anything more that people ask for!

## Testing

Given that I have been solely responsible for the majority of the code in the project up to date,
I've had to lean heavily on LLMs in order to assist me with architectural planning and coding
velocity (or this would never get done, I have a job). When speaking to others about StreamFusion,
their first question tends to be "how can you trust it?" In my opinion, StreamFusion is a good
project to build with AI because it is reimplementing something that already exists with a large
suite of test cases.

In order to test this code, I've built out a big data parity harness between Flink and StreamFusion
(both in output, intermediate results, and scaling/routing semantics). Additionally, I hook into
existing Flink SQL tests (and connector Flink SQL tests), and include them in my CI (this was
inspired by DataFusion Comet). Inevitably, like all projects, there will be bugs that come up.
However, I'd be lying if I said that I could write much more thorough code by hand.

## Benchmarks

I've tried to follow the example of Will Manning, founder of Spiral, when he says that he likes to
"steelman" his benchmarks. In order to do so, I've deliberately tested the worst-case scenario for
StreamFusion, which is when row-oriented data comes in via source and leaves via sink. To that end,
I have a Nexmark (standard streaming benchmark) suite taking in four partitions of Kafka data and
outputting it as well, using exactly-once checkpointing semantics. You'll find more information in
the project docs, but StreamFusion is already achieving over 33% more throughput than Flink with
byte-identical result sets. And this is in version 0.1.0, where I've deliberately chosen to avoid
some optimizations that come at the cost of increased code complexity. I think that a 2x target
across the board is an easy goal to hit.

## AI

In the past few months I've been fortunate enough to contribute to Iceberg, DataFusion, Comet,
Arrow-Java, and Paimon. One common theme I've noticed between them is that reviewer bandwidth is
limited - especially in the AI age where writing code has become faster than ever...and harder to
read than ever. As a contributor, reviewer bandwidth/feedback loops (or lack thereof) is the
biggest source of discouragement for me when debating whether to contribute to open-source
projects. I want to try and avoid that pain point for StreamFusion.

I've spoken to some experienced OSS contributors and they've raised some really fantastic points
about why a human should be in the loop - we're building software at scale and the cost of broken
trust is especially high. Architectural decisions that get made have to be maintained for years to
come.

I think that StreamFusion is unique in the sense that it is an accelerator project. There are
certainly architectural decisions to be made, but at the end of the day, StreamFusion doesn't
decide its own public-facing API. Flink does. And Flink still has a community of seasoned OSS
developers giving it a long-term strategic direction.

On the contrary, StreamFusion has a very clear goal of "exact Flink parity, as fast as possible
under the hood." To me this is the perfect type of problem for an AI to work on and optimize. It
has a baseline to compare results against, and at least up to this point, has done a fantastic job
of getting there. There are obviously architectural decisions that I have made/will continue to
make, but at this point I care more about having a discussion with a contributor and picking apart
higher-level decisions, than attempting to get super in the weeds about code style.

I hope to establish a code review process that involves humans signing off on a
planning/architectural document first, and then AI reviewing the code itself - ensuring that it
sticks to the plan, and reverting back to humans for any ambiguity and judgment calls. I believe
that with a suite of high-quality unit and integration tests we can ensure that this project stays
in line, and that we fix bugs fast when they're discovered.

## Future Work

We're just getting started! First of all, I plan to write a lot more blog posts covering our
detailed architecture. I think it'll help attract interest from others as well as ensure that I'm
staying on top of what's built (it can be all too tempting to let AI take the wheel at times). More
importantly, we need to build out more functionality. I have high conviction that my first targeted
use case (Kafka source, Kafka sink) is actually the one where StreamFusion provides the least
benefit to the end user. Situations where at least one expensive source or sink operator is
inherently columnar (Parquet/ORC/Vortex/Lance, Apache Fluss brokers, ADBC) is where StreamFusion will
really earn its keep, and hopefully justify adoption. As time goes by, there will only be more and
more data - our streaming systems will need to keep up.

One thing that I refuse to outsource to AI is my prose (thanks Bret Stephens), and I hope to keep
releasing blog posts like these in the near future.
