import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest


class PackageReleaseTest(unittest.TestCase):
    def test_output_paths_and_qualified_artifacts(self):
        script = Path(__file__).with_name("package-release.sh")
        for line in ("2.2", "1.18"):
            for relative in (True, False):
                with self.subTest(line=line, relative=relative), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp) / "repository"
                    (root / "bin").mkdir(parents=True)
                    shutil.copy2(script, root / "bin/package-release.sh")
                    for name in ("LICENSE", "readme.md"):
                        (root / name).write_text(name)
                    suffix = "-flink1.18" if line == "1.18" else ""
                    version = "0.1.0-test"
                    modules = ["loader", "core", "kafka", "json", "csv", "raw", "avro",
                               "avro-confluent-registry", "protobuf", "parquet", "orc", "paimon"]
                    if line == "2.2":
                        modules.append("delta")
                    artifacts = []
                    for module in modules:
                        target = root / f"streamfusion-{module}/target"
                        target.mkdir(parents=True)
                        classifier = "-runtime" if module == "core" else ""
                        artifact = f"streamfusion-{module}{suffix}-{version}{classifier}.jar"
                        (target / artifact).write_text(module)
                        artifacts.append(artifact)
                    mock_bin = Path(tmp) / "mock-bin"
                    mock_bin.mkdir()
                    mvn = mock_bin / "mvn"
                    mvn.write_text(f"#!/bin/sh\nprintf '%s\\n' '{version}'\n")
                    mvn.chmod(0o755)
                    env = dict(os.environ, PATH=str(mock_bin) + os.pathsep + os.environ["PATH"])
                    destination = Path("output with spaces")
                    if not relative:
                        destination = Path(tmp) / destination
                    result = subprocess.run(
                        [str(root / "bin/package-release.sh"), "--flink-line", line, str(destination)],
                        cwd=tmp, env=env, text=True, capture_output=True, check=True,
                    )
                    bundle = f"streamfusion{suffix}-{version}"
                    archive = Path(tmp) / destination / f"{bundle}-bin.tar.gz"
                    self.assertEqual(str(archive), result.stdout.strip())
                    with tarfile.open(archive) as packed:
                        self.assertEqual(
                            {f"{bundle}/{name}" for name in ["LICENSE", "readme.md", *artifacts]},
                            {member.name for member in packed.getmembers() if member.isfile()},
                        )
                    checksum = Path(str(archive) + ".sha256").read_text().split()[0]
                    self.assertEqual(hashlib.sha256(archive.read_bytes()).hexdigest(), checksum)


if __name__ == "__main__":
    unittest.main()
