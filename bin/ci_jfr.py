"""Condense real JFR samples without retaining large XML exports or recordings."""

from collections import Counter
import re
import subprocess
import xml.etree.ElementTree as ET


EVENTS = "jdk.ExecutionSample,jdk.NativeMethodSample,jdk.Compilation,jdk.ObjectAllocationSample"


def duration(value):
    match = re.fullmatch(r"PT(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?", value)
    if not match:
        raise ValueError(f"Invalid JFR duration: {value}")
    return sum(float(part or 0) * unit for part, unit in zip(match.groups(), (3600, 60, 1)))


def collect(stream, counters):
    for _, event in ET.iterparse(stream, events=("end",)):
        if event.tag != "event":
            continue
        kind = event.get("type")
        counters["events"][kind] += 1
        if kind in ("jdk.ExecutionSample", "jdk.NativeMethodSample"):
            frames = []
            for method in event.findall("./struct[@name='stackTrace']/array[@name='frames']/struct/struct[@name='method']"):
                owner = method.findtext("./struct[@name='type']/value[@name='name']", "<unknown>")
                name = method.findtext("./value[@name='name']", "<unknown>")
                frames.append(owner.replace("/", ".") + "." + name)
            if frames:
                counters["leaf_frames"][frames[0]] += 1
                counters["inclusive_frames"].update(set(frames))
            thread = event.findtext("./struct[@name='sampledThread']/value[@name='javaName']", "<unknown>")
            counters["sampled_threads"][thread] += 1
        elif kind == "jdk.Compilation":
            compiler = event.findtext("./value[@name='compiler']", "<unknown>")
            counters["compilation_events"][compiler] += 1
            counters["compilation_activity_seconds"][compiler] += duration(event.findtext("./value[@name='duration']"))
        elif kind == "jdk.ObjectAllocationSample":
            name = event.findtext("./struct[@name='objectClass']/value[@name='name']", "<unknown>")
            counters["allocation_sample_weight_bytes"][name] += int(event.findtext("./value[@name='weight']", "0"))
        event.clear()


def summarize(recordings, java_home):
    counters = {key: Counter() for key in ("events", "leaf_frames", "inclusive_frames", "sampled_threads",
                                          "compilation_events", "compilation_activity_seconds",
                                          "allocation_sample_weight_bytes")}
    if len(recordings) != 4:
        raise ValueError(f"Expected four fresh recorded test JVMs, found {len(recordings)}")
    for recording in recordings:
        command = [str(java_home / "bin/jfr"), "print", "--xml", "--events", EVENTS,
                   "--stack-depth", "32", str(recording)]
        with subprocess.Popen(command, stdout=subprocess.PIPE) as process:
            try:
                collect(process.stdout, counters)
            except BaseException:
                process.kill()
                process.wait()
                raise
            if process.wait():
                raise RuntimeError(f"Cannot read JFR recording: {recording}")
    if not counters["events"]["jdk.ExecutionSample"] or not counters["events"]["jdk.Compilation"]:
        raise ValueError("Missing CPU or compiler events in JFR recordings")
    result = {key: dict(counter.most_common(40)) for key, counter in counters.items()}
    result["recordings"] = len(recordings)
    result["limits"] = ("Leaf/inclusive frame counts are sampled Java stack attribution, not exact CPU time. "
                        "Inclusive counts overlap. Native samples do not resolve Rust internals. "
                        "Compilation durations are activity intervals, not compiler CPU seconds. "
                        "Allocation weights are sampled estimates. Instrumented elapsed times are not speedup evidence.")
    for recording in recordings:
        recording.unlink()
    return result


def configuration(java_home, output):
    tree = ET.parse(java_home / "lib/jfr/profile.jfc")
    for event in tree.getroot().findall("event"):
        for setting in event.findall("setting"):
            if event.get("name") == "jdk.Compilation" and setting.get("name") == "threshold":
                setting.text = "0 ms"
            if event.get("name") == "jdk.ObjectAllocationSample" and setting.get("name") == "throttle":
                setting.text = "25/s"
            if event.get("name") == "jdk.NativeMethodSample" and setting.get("name") == "period":
                setting.text = "10 ms"
    tree.write(output, encoding="utf-8", xml_declaration=True)
