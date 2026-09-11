# Releasing

StreamFusion publishes the Java reactor to Maven Central and attaches a universal deployment
bundle to the matching GitHub release. Releases are immutable: prepare and verify a version in a
commit before creating its tag.

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
rg '0\.1\.0-rc1'
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
git tag -s dry-run-v0.1.0-rc2 -m 'Dry run StreamFusion 0.1.0-rc2'
git push upstream dry-run-v0.1.0-rc2
```

The `dry-run-v<version>` path runs the same Linux and macOS runner builds, artifact checks, signing,
and Central upload. Central validates the deployment but does not publish it, and GitHub creates a
draft release whose assets are visible only to repository collaborators. Inspect the deployment in
the [Central Portal](https://central.sonatype.com/publishing/deployments), then drop it before using
the coordinate in a real release. Delete the draft release and dry-run tag after inspection.

Once the dry run passes, push the signed version tag only after the version commit is on `main`:

```sh
git tag -s v0.1.0-rc2 -m 'StreamFusion 0.1.0-rc2'
git push upstream v0.1.0-rc2
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
runner and the Apple Silicon payload on a macOS runner. It merges those binaries into the release
JARs, validates the artifact boundaries, signs and publishes the reactor through the Central Portal,
and only then creates the GitHub release. A version containing a hyphen, such as `0.1.0-rc2`, becomes
a GitHub prerelease.

If a release fails before Central reports it as published, fix the cause, delete the unpublished tag,
and prepare a new candidate version. Once Central has published a coordinate, never reuse it; advance
to the next candidate or patch version.
