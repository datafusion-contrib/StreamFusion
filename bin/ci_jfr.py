"""Condense real JFR samples without retaining large XML exports or recordings."""

from collections import Counter
import json
from pathlib import Path
import subprocess
import xml.etree.ElementTree as ET


def collect(stream, counters):
    for line in stream:
        event = json.loads(line)
        kind = event[0]
        counters["events"][kind] += 1
        if kind in ("jdk.ExecutionSample", "jdk.NativeMethodSample"):
            prefix = "java" if kind == "jdk.ExecutionSample" else "native"
            frames = event[2]
            if frames:
                counters[prefix + "_leaf_frames"][frames[0]] += 1
                counters[prefix + "_inclusive_frames"].update(set(frames))
            counters[prefix + "_sampled_threads"][event[1]] += 1
        elif kind == "jdk.Compilation":
            compiler = event[1]
            counters["compilation_events"][compiler] += 1
            counters["compilation_activity_seconds"][compiler] += float(event[2])
        elif kind == "jdk.ObjectAllocationSample":
            counters["allocation_sample_weight_bytes"][event[1]] += int(event[2])
        elif kind in ("jdk.ThreadPark", "jdk.ThreadSleep", "jdk.JavaMonitorWait"):
            seconds = float(event[2])
            counters["wait_seconds_by_kind"][kind] += seconds
            counters["wait_seconds_by_thread"][event[1]] += seconds
            if event[1] == "main":
                counters["main_wait_seconds_by_stack"][" > ".join(event[3][:8])] += seconds
        elif kind == "jdk.GarbageCollection":
            counters["gc_events"][event[1]] += 1
            counters["gc_activity_seconds"][event[1]] += float(event[2])
        elif kind == "jdk.GCHeapSummary":
            counters["gc_heap_used_max_bytes"][event[1]] = max(
                counters["gc_heap_used_max_bytes"][event[1]], int(event[2]))


def summarize(recordings, java_home, expected_recordings=4):
    counters = {key: Counter() for key in ("events", "java_leaf_frames", "java_inclusive_frames", "java_sampled_threads",
                                          "native_leaf_frames", "native_inclusive_frames", "native_sampled_threads",
                                          "compilation_events", "compilation_activity_seconds",
                                          "allocation_sample_weight_bytes", "wait_seconds_by_kind",
                                          "wait_seconds_by_thread", "main_wait_seconds_by_stack",
                                          "gc_events", "gc_activity_seconds", "gc_heap_used_max_bytes")}
    if len(recordings) != expected_recordings:
        raise ValueError(f"Expected {expected_recordings} fresh recorded test JVMs, found {len(recordings)}")
    for recording in recordings:
        command = [str(java_home / "bin/java"), "-Xmx2g", "--source", "11",
                   str(Path(__file__).with_name("CiJfrEvents.java")), str(recording)]
        with subprocess.Popen(command, stdout=subprocess.PIPE, text=True) as process:
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
    result["limits"] = ("Java execution samples and native-method stack samples are separate. "
                        "Native samples include blocking I/O and are not CPU attribution. "
                        "Inclusive counts overlap. Neither sample count is exact CPU time. "
                        "Native samples do not resolve Rust internals. "
                        "Compilation durations are activity intervals, not compiler CPU seconds. "
                        "Allocation weights are sampled estimates. Wait durations exclude unrecorded short waits, "
                        "overlap across threads and are not CPU time. GC activity is not total stop-the-world pause time; "
                        "heap summaries are maxima at recorded GC boundaries, not peak RSS. "
                        "Instrumented elapsed times are not speedup evidence.")
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
