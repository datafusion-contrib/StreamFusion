# CI setup and failure handling

Ordinary CI runs Rust tests, release Criterion fixture checks, Java tests on Flink 2.2 and
1.18, optional lake and format/connector suites, and optimized image integration tests.
The upstream Flink workflow runs separately; both aggregate gates must pass before merging.
Confirm the actual post-merge workflows as well as the PR checks when verifying a repair.

## Maven dependencies

Java CI jobs resolve their selected reactor's dependencies before building or testing:

```sh
python3 bin/ci-maven-dependencies.py --projects streamfusion-runtime
python3 bin/ci-maven-dependencies.py --profile flink-1.18 --projects streamfusion-runtime
```

The helper invokes the released Maven dependency plugin's `go-offline` goal, which resolves
project, plugin and report dependencies without invoking a build or test lifecycle. It keeps
the job's profiles, project selection and reactor dependencies. The settings file uses the
project's public repositories without adding mirrors or changing dependency versions.

Resolution has three attempts, with waits of 5 and 15 seconds and a ten-minute limit per
attempt. Each attempt uses Maven's `-U` option to recheck missing releases, including a lookup
that failed during a previous attempt. Persistent download failures, invalid dependencies and
timeouts remain blocking. Once resolution succeeds, the existing build and test commands run
once with their original assertions and failure handling. Tests are never retried by this helper;
their commands still use normal Maven resolution for any artifacts requested dynamically by plugins.

[PR #311's post-merge row-format job](https://github.com/datafusion-contrib/StreamFusion/actions/runs/37938400080/job/113846210991)
failed before compilation because one runner reported published Flink 2.2.1 and Kafka dependencies
as missing. Other jobs resolved those releases successfully. Separating downloads from tests
allows transient resolution failures to recover without rerunning test code; forcing missing-release
checks prevents Maven's failed-lookup cache from defeating the next setup attempt.

The upstream suite transfers its prepared pinned Maven distribution to its consumer runners
and verifies that it starts with downloads blocked. See the
[shared-build guide](upstream-flink-suite.md#shared-builds-and-runtime-shards).
