# Releasing

StreamFusion publishes the Java reactor, including the optional Delta and Paimon modules, to Maven
Central and attaches a universal deployment bundle to the matching GitHub release. The release
builder and publisher explicitly enable the `delta,paimon` profiles. Artifact checks verify Paimon's
separate native library and that Delta shares the Parquet module without bundling another library.
Releases are immutable: prepare and verify a version in a commit before creating its tag.

Every runtime payload carries `StreamFusion-Module` and `StreamFusion-Flink-Line` manifest entries.
`bin/check-artifacts.sh` verifies them against the artifact name and the selected build line, in
addition to checking native payload boundaries. The loader also validates the line at startup, so
release all modules from the same build; an unmarked older payload cannot be mixed into a newly
built installation. The current release target remains Flink 2.2.

## One-time GitHub setup

Create a `release` environment in the canonical GitHub repository. It may have required reviewers;
the workflow waits at that boundary before it can access credentials or publish anything. Add these
repository or environment secrets:

| Secret | Value |
| --- | --- |
| `CENTRAL_USERNAME` | Username from a Central Portal user token |
| `CENTRAL_PASSWORD` | Password from the same Central Portal user token |
| `MAVEN_GPG_PRIVATE_KEY` | ASCII-armored private key from `gpg --armor --export-secret-keys KEY_ID` |
| `MAVEN_GPG_PASSPHRASE` | Passphrase for that private key |

The Central account must have the verified `tech.streamfusion` namespace. Publish the signing
key's public half from the primary signing key to a Central-supported keyserver before the first
release:

```sh
gpg --keyserver keyserver.ubuntu.com --send-keys KEY_ID
```

## Prepare a version

Maven and Cargo versions deliberately live in source control so the JVM/native compatibility stamp
matches the immutable coordinate. Update all occurrences of the prior version, including the root
and loader POMs, `native/Cargo.toml`, `native/Cargo.lock`, deployment examples, and the upstream-suite
classpath POM. Confirm there are no stale values and run the normal release gates:

```sh
rg '0\.1\.0-rc2'
mvn test
bin/build-release.sh --host-only
bin/check-artifacts.sh --host-only
```

`bin/build-release.sh` packages with the unsigned `release` profile, so the source and javadoc
attachments the publish workflow builds are verified locally before any tag exists. Javadoc itself is
also generated during every `mvn test`, because the shared source root lets javadoc see the optional
connector sources (Delta, Paimon) beside each module's own: a reference javadoc cannot resolve fails
the ordinary test build rather than the release, and `-Dmaven.javadoc.skip=true` skips it for a
quick loop.

## Publish

Before publishing the first candidate, push a signed dry-run tag to the canonical repository:

```sh
git tag -s dry-run-v0.1.0-rc3 -m 'Dry run StreamFusion 0.1.0-rc3'
git push upstream dry-run-v0.1.0-rc3
```

The `dry-run-v<version>` path runs the same Linux and macOS runner builds, artifact checks, signing,
and Central upload. Central validates the deployment but does not publish it, and GitHub creates a
draft release whose assets are visible only to repository collaborators. Inspect the deployment in
the [Central Portal](https://central.sonatype.com/publishing/deployments), then drop it before using
the coordinate in a real release. Delete the draft release and dry-run tag after inspection.

Once the dry run passes, push the signed version tag only after the version commit is on `main`:

```sh
git tag -s v0.1.0-rc3 -m 'StreamFusion 0.1.0-rc3'
git push upstream v0.1.0-rc3
```

The release workflow rejects either tag form unless its value exactly matches both Maven projects
and Cargo.

All native packages inherit the single version in `native/Cargo.toml`'s `[workspace.package]`.
The release builder selects packages, producing `libstreamfusion` for the engine and a separately
named library for every native extension. Linux builds the workspace together; macOS selects the
same packages for each target. Shared dependencies are reused by Cargo. The staged resource layout
and Maven artifact names remain the same, including the single Avro native payload shared with
Avro-Confluent-Registry. Extension libraries are checked for foreign JNI entry points before shipping.

Following DataFusion Comet's runner-native pattern, it builds the Linux x86_64 payload on an Ubuntu
22.04 runner and the Apple Silicon payload on a macOS runner. The Linux image checks use that same
glibc 2.35 build baseline with a separate Rust cache key, preventing reuse of Ubuntu 24.04
objects. Linux artifact validation rejects packaged libraries requiring a newer glibc before
image execution. This baseline loads in the official Flink 1.18 and 2.2 images. The containerized
cross-platform builder uses Rust 1.94 on Debian Bullseye to stay below that ABI floor. A `--host-only`
build inherits its host's libc requirements; do not build a deployment for an older distribution
on Ubuntu 24.04. Both Java payload lines use the host SLF4J 1.7 API and provider, avoiding a
conflicting SLF4J 2 API in Flink’s global classpath. The workflow merges those binaries into the release
JARs, validates the artifact boundaries, signs and publishes the reactor through the Central Portal,
and only then creates the GitHub release. A version containing a hyphen, such as `0.1.0-rc3`, becomes
a GitHub prerelease.

If a release fails before Central reports it as published, fix the cause, delete the unpublished tag,
and prepare a new candidate version. Once Central has published a coordinate, never reuse it; advance
to the next candidate or patch version.

## Flink 1.18 development artifacts

`-Pflink-1.18` selects Flink 1.18.1 and adds `-flink1.18` to each deployment artifact ID. Build
the line in a clean output tree and check it with
`bin/check-artifacts.sh --flink-line 1.18` (`--host-only` for a local single-platform build).
The default 2.2 artifacts keep their existing coordinates. Never combine outputs from the two
profiles into one archive or installation. The published POMs must contain the resolved qualified
coordinates and selected dependency versions, including inherited Arrow dependencies. The
flattened module POMs are checked alongside the JARs so a successful reactor build cannot hide
missing dependencies from downstream consumers.

Publication of the 1.18 line remains gated on
[dual-line CI and release validation](https://github.com/datafusion-contrib/StreamFusion/issues/189),
the [connector matrix](https://github.com/datafusion-contrib/StreamFusion/issues/187) and
[real-cluster recovery checks](https://github.com/datafusion-contrib/StreamFusion/issues/188).
The local release tools accept the same line explicitly:

```sh
bin/build-release.sh --host-only --flink-line 1.18
bin/check-artifacts.sh --host-only --flink-line 1.18
bin/package-release.sh --flink-line 1.18
```

Release archives omit macOS metadata sidecars so they contain the same intended files on every
build host. The 1.18 archive has a `streamfusion-flink1.18-` prefix and contains only qualified payloads;
Delta is excluded from its build and archive. Base-image smoke validation covers both lines;
the automated publication workflow still targets 2.2 until the remaining gates pass.
No 1.18 Delta artifact is currently admitted. See
[Flink line compatibility](flink-compatibility.md) for the exact development scope.
