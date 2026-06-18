This project attempts to run Apache Flink using Apache DataFusion + rust operators via JNI to accelerate stream
processing throughput. It is a very similar project to DataFusion comet in spirit, but with stream processing.

We are aiming for IDENTICAL results in stream processing jobs to flink. To start, we'll focus on Flink SQL. As column
oriented streaming sources pop up more and more (see Fluss, for example, or open table CDC), being able to run jobs
at massive throughput becomes more important.

This project is meant to be open sourced. For each commit, I want small, targeted diffs with a clear purpose. Commit
messages should be used as the sole source of truth for developer-facing documentation. They should be more
architectural in nature - do not name specific classes that the average developer does not know off the top of their
head, but instead concisely explain the "why" of the change and the reasons for our architecture.

When reviewing code, make sure that it follows the existing principles of codebases which we will take influence from:
- Flink (see ~/data/flink for code)
- DataFusion (see ~/data/datafusion for code)
- DataFusion comet (see ~/data/datafusion-comet for code)
- Arroyo (see ~/data/arroyo for code)

Because of AI code reviews, we anticipate that there will be an influx of code. I want humans to agree on the
architecture of a solution, and then allow AI to ensure that the written code is clean and readable by any human.
Comments should not be necessary unless they explain something non-obvious to the reader. Follow typical clean code
principles like DRY and KISS. All changes should be tested, and you should look for uncovered significant edge cases
in tests. When adding new functionality to accelerate streaming, we should be able to benchmark it vs. before and
add those improvements to our commit message. If our benchmarks don't improve, we should seriously reconsider whether
the feature is worth it, or if it is the precursor to more optimizations.

At a high level:
We are ripping code out of Arroyo, which itself already uses DataFusion
We are overriding the planning layer of Flink to use our Arroyo operators
This allows us to handle incoming records is arrow batches as opposed to using the flink row model

The research directory in this folder includes the learnings of previous sessions when looking into other
repos/techniques. See `.claude/research/flink-arroyo-accelerator-findings.md` for the full architecture
investigation (planner hook, JNI/Arrow bridge, memory accounting, threading/mailbox model, changelog and
watermark semantics, type mapping, parity testing, and a risk-first build order).

The todos directory in this folder is effectively a JIRA board of tickets to complete with context on them.
These can be deleted as soon as tickets get knocked out. As we knock things out, we should add to the "Compatibility
Chart" section of the readme.md so we know what operators are currently accelerated and the terms under which they
are.