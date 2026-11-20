# Command entry points

Keep stable developer commands here. Python implementations and their unit tests belong to
`dev/tools/`; upstream-suite phases belong to `dev/flink-suite/runner/`. These are development
tools, not files to install in Flink's production `bin` directory.

See [the tool catalog](../docs/developer-tools.md) for responsibilities, requirements, outputs,
network access and commands that modify a checkout or publish results.

Run tool tests from the repository root:

```sh
python3 -m dev.tools.tests
python3 -m dev.tools.tests --list
```

The test command checks tool behavior; it does not authorize or launch performance jobs.
