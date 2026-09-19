import ctypes
import importlib.util
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile


spec = importlib.util.spec_from_file_location(
    "check_native_tls", Path(__file__).with_name("check-native-tls.py")
)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


@unittest.skipUnless(sys.platform == "linux", "ELF TLS regression requires Linux")
class NativeTlsTest(unittest.TestCase):
    def test_dynamic_loading_and_packaged_elf_flags(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "tls.c"
            source.write_text(
                "__thread volatile char state[16384];\n"
                "int probe(void) { state[0] = 42; return state[0]; }\n"
            )
            for model in ["initial-exec", "local-dynamic"]:
                with self.subTest(model=model):
                    library = root / f"{model}.so"
                    subprocess.run(
                        ["cc", "-shared", "-fPIC", f"-ftls-model={model}",
                         str(source), "-o", str(library)],
                        check=True,
                    )
                    archive = root / f"{model}.jar"
                    with zipfile.ZipFile(archive, "w") as jar:
                        jar.write(library, "tech/streamfusion/native/linux/x86_64/library.so")
                    if model == "initial-exec":
                        with self.assertRaisesRegex(ValueError, "requires static TLS"):
                            checker.check_archive(archive)
                        with self.assertRaisesRegex(OSError, "static TLS"):
                            ctypes.CDLL(str(library))
                    else:
                        checker.check_archive(archive)
                        # Distinct paths emulate separate planner/job classloaders.
                        copies = []
                        for index in range(4):
                            copy = root / f"copy-{index}.so"
                            shutil.copyfile(library, copy)
                            loaded = ctypes.CDLL(str(copy))
                            self.assertEqual(42, loaded.probe())
                            copies.append(loaded)


if __name__ == "__main__":
    unittest.main()
