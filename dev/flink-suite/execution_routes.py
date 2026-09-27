"""Validate job work before assigning routes from completed execution evidence."""
from collections import Counter
import re


def work_counts(value):
    if not isinstance(value, dict) or any(
            not isinstance(name, str) or not re.fullmatch(r'\w+', name)
            or type(count) is not int or count <= 0 for name, count in value.items()):
        raise ValueError('missing or invalid native-work observations')
    return value


def validate_partition(row):
    """Older inventories lack the partition; they cannot establish a runtime route."""
    unattributed = row.get('unattributed_native_work')
    unmatched = row.get('unmatched_native_jobs')
    if unattributed is None and unmatched is None:
        return False
    total = Counter(work_counts(unattributed))
    jobs = row.get('jobs')
    if not isinstance(jobs, dict) or not isinstance(unmatched, dict):
        raise ValueError('missing or invalid job-work partition')
    for job, counts in unmatched.items():
        if not isinstance(job, str) or not job or job in jobs:
            raise ValueError('invalid or duplicated unmatched job identity')
        total.update(work_counts(counts))
    for result in jobs.values():
        total.update(work_counts(result.get('native_work', {})))
    if dict(total) != row['native_work']:
        raise ValueError('job-work partition differs from invocation native work')
    return True


def classify(row, partition_valid):
    if row['outcome'] == 'skipped':
        return 'skipped'
    jobs = row.get('jobs')
    if not partition_valid or not jobs or row['native_work']:
        return 'unclassified'
    failed = False
    all_batch = True
    for result in jobs.values():
        graph = result.get('graph')
        confirmed_failure = (result['status'] == 'RESULT_FAILED'
                             and result.get('job_status') == 'FAILED'
                             and not result.get('job_status_error'))
        if not confirmed_failure and (result['status'] != 'SUCCEEDED'
                                     or result.get('job_status') in ('FAILED', 'CANCELED', 'SUSPENDED')):
            return 'unclassified'
        if not isinstance(graph, dict):
            return 'unclassified'
        failed |= confirmed_failure
        nodes = graph.get('nodes')
        if graph.get('job_type') not in ('BATCH', 'STREAMING') or graph.get('observation_error') or not nodes:
            return 'unclassified'
        all_batch &= graph['job_type'] == 'BATCH'
        if not isinstance(nodes, list) or any(
                not isinstance(node, dict) or node.get('operator_class_error')
                or not isinstance(node.get('operator_class'), str)
                or not node['operator_class'].startswith('org.apache.flink.') for node in nodes):
            return 'unclassified'
    return 'host_failure' if failed else 'batch_host_only' if all_batch else 'unclassified'
