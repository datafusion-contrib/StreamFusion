#!/usr/bin/env python3
"""Check deployment bytecode: JDK 11, or JDK 17 for the optional Iceberg adapter."""
import argparse
import io
from pathlib import Path
import struct
import zipfile


def inspect_jar(archive, origin, java_version=11):
    failures = []
    checked = 0
    with zipfile.ZipFile(archive) as jar:
        manifest = jar.read('META-INF/MANIFEST.MF').decode('utf-8', errors='replace') if 'META-INF/MANIFEST.MF' in jar.namelist() else ''
        manifest = manifest.replace('\r\n ', '').replace('\n ', '')
        multi_release = any(line.lower().strip() == 'multi-release: true' for line in manifest.splitlines())
        selected = {}
        for name in jar.namelist():
            if name.endswith('.jar'):
                count, errors = inspect_jar(io.BytesIO(jar.read(name)), f'{origin}!/{name}', java_version)
                checked += count
                failures.extend(errors)
            if not name.endswith('.class'):
                continue
            version = 0
            logical_name = name
            if name.startswith('META-INF/versions/'):
                parts = name.split('/', 3)
                if not multi_release or len(parts) != 4 or not parts[2].isdigit():
                    continue
                version = int(parts[2])
                if version < 9 or version > java_version:
                    continue
                logical_name = parts[3]
            if version >= selected.get(logical_name, (-1, ''))[0]:
                selected[logical_name] = (version, name)
        for _, name in selected.values():
            with jar.open(name) as entry:
                header = entry.read(8)
            if len(header) != 8 or header[:4] != b'\xca\xfe\xba\xbe':
                failures.append(f'{origin}!/{name}: invalid class header')
                continue
            major = struct.unpack('>H', header[6:8])[0]
            checked += 1
            if major > java_version + 44:
                failures.append(f'{origin}!/{name}: class version {major}, maximum {java_version + 44}')
    return checked, failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('.'))
    args = parser.parse_args()
    jars = sorted(p for p in args.root.glob('streamfusion-*/target/*.jar')
                  if not any(p.name.endswith(suffix) for suffix in ('-sources.jar', '-javadoc.jar', '-tests.jar')))
    if not jars:
        parser.error('No deployment JARs found; build the Maven reactor first')
    checked = 0
    failures = []
    for jar in jars:
        java_version = 17 if jar.parent.parent.name == "streamfusion-iceberg" else 11
        count, errors = inspect_jar(jar, str(jar), java_version)
        checked += count
        failures.extend(errors)
    if failures:
        raise SystemExit('\n'.join(failures))
    print(f'Deployment bytecode: {checked} classes in {len(jars)} JARs passed (JDK 11; Iceberg JDK 17)')


if __name__ == '__main__':
    main()
