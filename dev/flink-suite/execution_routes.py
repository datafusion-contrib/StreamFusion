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


SOURCE_OPERATORS = {
    'org.apache.flink.streaming.api.operators.StreamSource',
    'org.apache.flink.streaming.api.operators.SourceOperator',
}
SINK_OPERATORS = {
    'org.apache.flink.streaming.api.operators.StreamSink',
    'org.apache.flink.streaming.api.operators.collect.CollectSinkOperator',
    'org.apache.flink.streaming.runtime.operators.sink.SinkWriterOperator',
}
NATIVE_OPERATORS = {
    'tech.streamfusion.operator.' + name for name in (
        'NativeCalcOperator', 'NativeLookupJoinOperator', 'NativeAsyncLookupJoinOperator',
        'NativeFilterOperator', 'NativeColumnarGroupAggregateOperator',
        'NativeColumnarUpdatingJoinOperator', 'NativeColumnarTopNOperator',
        'NativeColumnarGlobalWindowAggregateOperator')
}
BOUNDARY_OPERATORS = SOURCE_OPERATORS | SINK_OPERATORS | {
    'tech.streamfusion.operator.RowDataToArrowOperator',
    'tech.streamfusion.operator.ArrowToRowDataOperator',
    'tech.streamfusion.operator.SplitByKeyGroupOperator',
    'tech.streamfusion.operator.OrderedKeyGroupReassembler',
}


def job_route(result):
    graph = result.get('graph')
    confirmed_failure = (result['status'] == 'RESULT_FAILED'
                         and result.get('job_status') == 'FAILED'
                         and not result.get('job_status_error'))
    if not confirmed_failure and (result['status'] != 'SUCCEEDED'
                                 or result.get('job_status') in ('FAILED', 'CANCELED', 'SUSPENDED')):
        return 'unclassified'
    if not isinstance(graph, dict):
        return 'unclassified'
    nodes = graph.get('nodes')
    if graph.get('job_type') not in ('BATCH', 'STREAMING') or graph.get('observation_error') or not nodes:
        return 'unclassified'
    if not isinstance(nodes, list) or any(
            not isinstance(node, dict) or node.get('operator_class_error')
            or not isinstance(node.get('operator_class'), str) or not node['operator_class']
            for node in nodes):
        return 'unclassified'
    classes = {node['operator_class'] for node in nodes}
    host_classes = {node['operator_class'] for node in nodes
                    if node['operator_class'].startswith('org.apache.flink.')
                    or node.get('factory') == 'org.apache.flink.table.runtime.operators.CodeGenOperatorFactory'}
    native = classes & NATIVE_OPERATORS
    host = classes - BOUNDARY_OPERATORS - native
    if not host <= host_classes:
        return 'unclassified'
    work = result.get('native_work', {})
    if work:
        expected = {name.rsplit('.', 1)[1] for name in native}
        if confirmed_failure or graph['job_type'] != 'STREAMING' or set(work) != expected:
            return 'unclassified'
        return 'mixed' if host else 'native'
    if native or not classes <= host_classes:
        return 'unclassified'
    if confirmed_failure:
        return 'host_failure'
    if graph['job_type'] == 'BATCH':
        return 'batch_host_only'
    if classes <= SOURCE_OPERATORS | SINK_OPERATORS and classes & SOURCE_OPERATORS and classes & SINK_OPERATORS:
        return 'scan_only'
    return 'host_only'


def classify(row, partition_valid):
    if row['outcome'] == 'skipped':
        return 'skipped'
    jobs = row.get('jobs')
    if (not partition_valid or not jobs or row.get('unattributed_native_work')
            or row.get('unmatched_native_jobs')):
        return 'unclassified'
    routes = {job_route(result) for result in jobs.values()}
    if 'unclassified' in routes:
        return 'unclassified'
    if routes == {'host_only'}:
        return 'unclassified'
    if len(routes) == 1:
        return routes.pop()
    if routes <= {'native', 'mixed', 'scan_only'}:
        return 'mixed'
    if 'host_failure' in routes and routes <= {'host_failure', 'batch_host_only', 'scan_only', 'host_only'}:
        return 'host_failure'
    return 'unclassified'
