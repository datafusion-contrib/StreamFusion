import importlib.util
import io
from pathlib import Path
import struct
import unittest
import zipfile

spec = importlib.util.spec_from_file_location('bytecode', Path(__file__).with_name('check-java-bytecode.py'))
bytecode = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bytecode)


def archive(entries, multi_release=False):
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w') as jar:
        if multi_release:
            jar.writestr('META-INF/MANIFEST.MF', 'Manifest-Version: 1.0\r\nMulti-Release: true\r\n\r\n')
        for name, value in entries.items():
            jar.writestr(name, struct.pack('>IHH', 0xcafebabe, 0, value) if isinstance(value, int) else value)
    output.seek(0)
    return output


class JavaBytecodeTest(unittest.TestCase):
    def test_rejects_newer_base_class_and_embedded_payload(self):
        payload = archive({'Native.class': 61}).getvalue()
        checked, failures = bytecode.inspect_jar(archive({'Old.class': 52, 'payload.jar': payload}), 'loader')
        self.assertEqual(2, checked)
        self.assertEqual(['loader!/payload.jar!/Native.class: class version 61, maximum 55'], failures)

    def test_selects_only_the_effective_multi_release_class(self):
        entries = {'Base.class': 61, 'META-INF/versions/11/Base.class': 55,
                   'META-INF/versions/17/Base.class': 61}
        self.assertEqual((1, []), bytecode.inspect_jar(archive(entries, True), 'module'))
        checked, failures = bytecode.inspect_jar(archive(entries), 'module')
        self.assertEqual(1, checked)
        self.assertEqual(1, len(failures))

    def test_java_11_override_must_itself_be_loadable(self):
        entries = {'Base.class': 52, 'META-INF/versions/11/Base.class': 61}
        checked, failures = bytecode.inspect_jar(archive(entries, True), 'module')
        self.assertEqual(1, checked)
        self.assertIn('META-INF/versions/11/Base.class', failures[0])


if __name__ == '__main__':
    unittest.main()
