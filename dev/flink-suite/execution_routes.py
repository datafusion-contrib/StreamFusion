"""Validate job work before assigning routes from completed execution evidence."""
from collections import Counter
import re


def work_counts(value):
    if not isinstance(value, dict) or any(
            not isinstance(name, str) or not re.fullmatch(r'\w+', name)
            or type(count) is not int or count <= 0 for name, count in value.items()):
        raise ValueError('missing or invalid native-work observations')
    return value


APPLICATION_STATUSES = {'SUCCEEDED', 'FAILED', 'CANCELED', 'UNKNOWN'}


def application_statuses(value):
    if (not isinstance(value, list) or not value
            or any(not isinstance(status, str) or status not in APPLICATION_STATUSES for status in value)
            or len(set(value)) != len(value)):
        raise ValueError('invalid application-status observations')
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
    unmatched_results = row.get('unmatched_job_results', {})
    if not isinstance(unmatched_results, dict):
        raise ValueError('invalid unmatched job results')
    for job, statuses in unmatched_results.items():
        if not isinstance(job, str) or not job or job in jobs:
            raise ValueError('invalid or duplicated unmatched result identity')
        application_statuses(statuses)
    for result in jobs.values():
        if 'application_statuses' in result:
            application_statuses(result['application_statuses'])
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
    'org.apache.flink.table.runtime.operators.sink.SinkOperator',
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


GENERATED_FACTORY = 'org.apache.flink.table.runtime.operators.CodeGenOperatorFactory'
SINK_ADAPTERS = {
    'org.apache.flink.table.runtime.operators.sink.OutputConversionOperator',
    'org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcer',
    'org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer',
}


def boundary_adapters(nodes):
    by_id = {}
    for node in nodes:
        identity = node.get('id')
        inputs = node.get('inputs')
        if (type(identity) is not int or identity in by_id or not isinstance(inputs, list)
                or any(type(source) is not int for source in inputs)):
            return set()
        by_id[identity] = node
    successors = {identity: [] for identity in by_id}
    for identity, node in by_id.items():
        for source in node['inputs']:
            if source not in by_id:
                return set()
            successors[source].append(identity)

    def generated(node, name):
        return node.get('factory') == GENERATED_FACTORY and re.fullmatch(
            name + r'\$[0-9]+', node['operator_class']) is not None

    def reaches_boundary(identity, source_side, visiting):
        if identity in visiting:
            return False
        node = by_id[identity]
        links = node['inputs'] if source_side else successors[identity]
        endpoints = SOURCE_OPERATORS if source_side else SINK_OPERATORS
        if node['operator_class'] in endpoints:
            return not links
        allowed = (generated(node, 'SourceConversion') if source_side else
                   node['operator_class'] in SINK_ADAPTERS or generated(node, 'SinkConversion'))
        return bool(allowed and links and all(
            reaches_boundary(parent, source_side, visiting | {identity}) for parent in links))

    return {identity for identity, node in by_id.items()
            if node['operator_class'] not in SOURCE_OPERATORS | SINK_OPERATORS
            and (reaches_boundary(identity, True, set()) or reaches_boundary(identity, False, set()))}


def linked_translations(row, result):
    graph = result.get('graph', {})
    ids = graph.get('sql_translation_ids')
    details = row.get('translation_details')
    if (graph.get('sql_translation_complete') is not True or not isinstance(ids, list) or not ids
            or any(type(identity) is not int or identity < 0 for identity in ids)
            or len(set(ids)) != len(ids) or not isinstance(details, list)):
        return []
    selected = []
    for identity in ids:
        matches = [detail for detail in details if isinstance(detail, dict)
                   and type(detail.get('id')) is int and detail['id'] == identity]
        if len(matches) != 1 or matches[0].get('status') != 'TRANSLATED':
            return []
        if type(matches[0].get('root_count')) is not int or matches[0]['root_count'] <= 0:
            return []
        selected.append(matches[0])
    return selected


def linked_fallback_reasons(row, result):
    plans = row.get('plans')
    if not isinstance(plans, list):
        return []
    reasons = []
    for detail in linked_translations(row, result):
        indices = detail.get('plan_indices')
        if (not isinstance(indices, list) or not indices
                or any(type(index) is not int or not 0 <= index < len(plans) for index in indices)
                or len(set(indices)) != len(indices)):
            return []
        for index in indices:
            plan = plans[index]
            if not isinstance(plan, dict) or plan.get('during_translation') is not True:
                return []
            roots = plan.get('roots')
            fallback = plan.get('fallback_reasons')
            if (not isinstance(roots, list) or not roots
                    or any(not isinstance(root, dict) or type(root.get('native_operators')) is not int
                           or root['native_operators'] != 0 for root in roots)
                    or not isinstance(fallback, list) or not fallback
                    or any(not isinstance(reason, str) or not reason.strip() for reason in fallback)):
                return []
            reasons.extend(fallback)
    return list(dict.fromkeys(reasons))


def job_route(result, row=None):
    graph = result.get('graph')
    applications = result.get('application_statuses', [])
    if len(applications) > 1:
        return 'unclassified'
    failed_result = applications == ['FAILED']
    job_status = result.get('job_status')
    confirmed_failure = (result['status'] == 'RESULT_FAILED'
                         and (not applications or failed_result)
                         and ((job_status == 'FAILED' and not result.get('job_status_error'))
                              or (failed_result and job_status not in
                                  ('FINISHED', 'CANCELED', 'SUSPENDED'))))
    if not confirmed_failure and (result['status'] != 'SUCCEEDED'
                                 or (applications and applications != ['SUCCEEDED'])
                                 or job_status in ('FAILED', 'CANCELED', 'SUSPENDED')):
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
                    or node.get('factory') == GENERATED_FACTORY}
    adapters = boundary_adapters(nodes)
    native = classes & NATIVE_OPERATORS
    host = {node['operator_class'] for node in nodes
            if node['operator_class'] not in BOUNDARY_OPERATORS | native
            and node.get('id') not in adapters}
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
    translations = linked_translations(row, result) if row is not None else []
    if translations and all(isinstance(detail.get('planner_configuration'), dict)
                            and detail['planner_configuration'].get('unmodified') is True
                            for detail in translations):
        return 'unmodified_plan'
    if (all(node['operator_class'] in SOURCE_OPERATORS | SINK_OPERATORS
            or node.get('id') in adapters for node in nodes)
            and classes & SOURCE_OPERATORS and classes & SINK_OPERATORS):
        return 'scan_only'
    if row is not None and linked_fallback_reasons(row, result):
        return 'full_fallback'
    return 'host_only'


def classify(row, partition_valid):
    if row['outcome'] == 'skipped':
        return 'skipped'
    jobs = row.get('jobs')
    if (not partition_valid or not jobs or row.get('unattributed_native_work')
            or row.get('unmatched_native_jobs') or row.get('unmatched_job_results')):
        return 'unclassified'
    routes = {job_route(result, row) for result in jobs.values()}
    if 'unclassified' in routes:
        return 'unclassified'
    if routes == {'host_only'}:
        return 'unclassified'
    if len(routes) == 1:
        return routes.pop()
    if routes <= {'full_fallback', 'scan_only'}:
        return 'full_fallback'
    if routes <= {'unmodified_plan', 'scan_only'}:
        return 'unmodified_plan'
    if routes <= {'native', 'mixed', 'scan_only', 'full_fallback'}:
        return 'mixed'
    if routes & {'native', 'mixed'} and routes <= {'native', 'mixed', 'scan_only', 'unmodified_plan', 'full_fallback'}:
        return 'mixed'
    if 'host_failure' in routes and routes <= {'host_failure', 'batch_host_only', 'scan_only', 'host_only', 'full_fallback', 'unmodified_plan'}:
        return 'host_failure'
    return 'unclassified'
