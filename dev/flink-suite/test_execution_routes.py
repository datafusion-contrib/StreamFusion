import copy
import unittest

import execution_routes as routes


class RuntimeRoutesTest(unittest.TestCase):
    SOURCE = 'org.apache.flink.streaming.api.operators.StreamSource'
    SINK = 'org.apache.flink.streaming.api.operators.collect.CollectSinkOperator'
    CALC = 'tech.streamfusion.operator.NativeCalcOperator'
    MAP = 'org.apache.flink.streaming.api.operators.StreamMap'

    def job(self, classes, work=None):
        return dict(status='SUCCEEDED', native_work=work or {},
                    graph=dict(job_type='STREAMING',
                               nodes=[dict(operator_class=name) for name in classes]))

    def classify(self, jobs, unattributed=None, unmatched=None):
        from collections import Counter
        work = Counter(unattributed or {})
        for counts in (unmatched or {}).values():
            work.update(counts)
        for job in jobs.values():
            work.update(job['native_work'])
        row = dict(outcome='passed', native_work=dict(work), jobs=jobs,
                   unattributed_native_work=unattributed or {}, unmatched_native_jobs=unmatched or {})
        return routes.classify(row, routes.validate_partition(row))

    def test_native_work_must_match_submitted_operator_types(self):
        for classes, work, expected in (
                ([self.SOURCE, self.CALC, self.SINK], {'NativeCalcOperator': 3}, 'native'),
                ([self.SOURCE, self.CALC, self.MAP, self.SINK], {'NativeCalcOperator': 3}, 'mixed'),
                ([self.SOURCE, self.CALC, self.SINK], {}, 'unclassified'),
                ([self.SOURCE, self.MAP, self.SINK], {'NativeCalcOperator': 3}, 'unclassified'),
                ([self.SOURCE, self.CALC, self.SINK], {'OtherNativeOperator': 3}, 'unclassified'),
                ([self.SOURCE, self.CALC, 'unknown.Operator', self.SINK],
                 {'NativeCalcOperator': 3}, 'unclassified'),
                ([self.SOURCE, self.CALC, 'tech.streamfusion.operator.NativeFilterOperator', self.SINK],
                 {'NativeCalcOperator': 3}, 'unclassified'),
                ([self.SOURCE, 'tech.streamfusion.operator.RowDataToArrowOperator', self.CALC,
                  'tech.streamfusion.operator.ArrowToRowDataOperator', self.SINK],
                 {'NativeCalcOperator': 3}, 'native')):
            with self.subTest(classes=classes, work=work):
                self.assertEqual(expected, self.classify({'job': self.job(classes, work)}))

    def test_scan_only_requires_both_source_and_sink_without_other_operators(self):
        for classes, expected in (([self.SOURCE, self.SINK], 'scan_only'),
                                  ([self.SOURCE], 'unclassified'), ([self.SINK], 'unclassified'),
                                  ([self.SOURCE, self.MAP, self.SINK], 'unclassified')):
            with self.subTest(classes=classes):
                self.assertEqual(expected, self.classify({'job': self.job(classes)}))

    def test_multiple_jobs_and_unassociated_work_cannot_hide_unknown_execution(self):
        native = self.job([self.SOURCE, self.CALC, self.SINK], {'NativeCalcOperator': 3})
        scan = self.job([self.SOURCE, self.SINK])
        self.assertEqual('mixed', self.classify({'native': native, 'scan': scan}))
        self.assertEqual('native', self.classify({'first': native, 'second': copy.deepcopy(native)}))
        self.assertEqual('unclassified', self.classify({'native': native}, {'NativeCalcOperator': 2}))
        self.assertEqual('unclassified', self.classify(
            {'native': native}, unmatched={'missing': {'NativeCalcOperator': 2}}))
        for key, value in (('status', 'SUBMITTED'), ('status', 'RESULT_FAILED')):
            pending = copy.deepcopy(scan)
            pending[key] = value
            self.assertEqual('unclassified', self.classify({'native': native, 'other': pending}))
        scan['graph']['job_type'] = 'BATCH'
        self.assertEqual('unclassified', self.classify({'native': native, 'batch': scan}))

    def test_failed_host_job_can_coexist_with_successful_host_streaming_job(self):
        success = self.job([self.SOURCE, self.MAP, self.SINK])
        failure = copy.deepcopy(success)
        failure.update(status='RESULT_FAILED', job_status='FAILED')
        self.assertEqual('host_failure', self.classify({'success': success, 'failure': failure}))

    def test_authoritative_job_result_survives_archived_status_lookup(self):
        job = self.job([self.SOURCE, self.MAP, self.SINK])
        job.update(status='RESULT_FAILED', application_statuses=['FAILED'],
                   job_status_error='JobNotFoundException')
        self.assertEqual('host_failure', self.classify({'failed': job}))
        for statuses in (['CANCELED'], ['UNKNOWN'], ['SUCCEEDED'], ['FAILED', 'SUCCEEDED']):
            with self.subTest(statuses=statuses):
                job['application_statuses'] = statuses
                self.assertEqual('unclassified', self.classify({'failed': job}))
        job['application_statuses'] = ['FAILED']
        for status in ('FINISHED', 'CANCELED', 'SUSPENDED'):
            job['job_status'] = status
            self.assertEqual('unclassified', self.classify({'failed': job}))
        job['status'] = 'SUCCEEDED'
        job.pop('job_status')
        self.assertEqual('unclassified', self.classify({'failed': job}))

    def test_unmatched_results_and_malformed_statuses_cannot_establish_a_route(self):
        job = self.job([self.SOURCE, self.SINK])
        row = dict(outcome='passed', native_work={}, jobs={'job': job},
                   unattributed_native_work={}, unmatched_native_jobs={},
                   unmatched_job_results={'missing': ['FAILED']})
        self.assertEqual('unclassified', routes.classify(row, routes.validate_partition(row)))
        for unmatched in ({'job': ['FAILED']}, {'missing': []}, {'missing': ['invented']}, []):
            row['unmatched_job_results'] = unmatched
            with self.assertRaises(ValueError):
                routes.validate_partition(row)
        row['unmatched_job_results'] = {}
        for statuses in ([], 'FAILED', ['FAILED', 'FAILED'], [1]):
            job['application_statuses'] = statuses
            with self.assertRaises(ValueError):
                routes.validate_partition(row)

    def test_generated_classes_require_the_released_flink_factory_identity(self):
        job = self.job([self.SOURCE, 'BatchExecCalc$2', self.SINK])
        job['graph']['job_type'] = 'BATCH'
        generated = job['graph']['nodes'][1]
        self.assertEqual('unclassified', self.classify({'batch': job}))

        generated['factory'] = 'other.CodeGenOperatorFactory'
        self.assertEqual('unclassified', self.classify({'batch': job}))
        generated['factory'] = 'org.apache.flink.table.runtime.operators.CodeGenOperatorFactory'
        self.assertEqual('batch_host_only', self.classify({'batch': job}))
        generated['operator_class'] = ''
        self.assertEqual('unclassified', self.classify({'batch': job}))

    def chain(self, classes, work=None):
        job = self.job(classes, work)
        for index, node in enumerate(job['graph']['nodes']):
            node.update(id=index, inputs=[] if index == 0 else [index - 1])
            if '$' in node['operator_class']:
                node['factory'] = routes.GENERATED_FACTORY
        return job

    def test_conversion_boundaries_require_graph_position(self):
        work = {'NativeCalcOperator': 3}
        for sink_conversion in ('SinkConversion$6',
                                'org.apache.flink.table.runtime.operators.sink.OutputConversionOperator'):
            job = self.chain([self.SOURCE, 'SourceConversion$4', self.CALC, sink_conversion, self.SINK], work)
            self.assertEqual('native', self.classify({'job': job}))
        # The same generated class at the source and inside the computation must not
        # make the interior instance disappear from the host portion of the graph.
        interior = self.chain([self.SOURCE, 'SourceConversion$4', self.CALC,
                               'SourceConversion$4', self.SINK], work)
        self.assertEqual('mixed', self.classify({'job': interior}))
        interior = self.chain([self.SOURCE, 'SinkConversion$6', self.CALC, self.SINK], work)
        self.assertEqual('mixed', self.classify({'job': interior}))
        missing_edges = self.chain([self.SOURCE, 'SourceConversion$4', self.CALC, self.SINK], work)
        del missing_edges['graph']['nodes'][1]['inputs']
        self.assertEqual('mixed', self.classify({'job': missing_edges}))

    def test_scan_only_can_include_verified_source_and_sink_conversions(self):
        job = self.chain([self.SOURCE, 'SourceConversion$4', 'SinkConversion$6', self.SINK])
        self.assertEqual('scan_only', self.classify({'job': job}))
        job['graph']['nodes'][-1]['operator_class'] = 'org.apache.flink.table.runtime.operators.sink.SinkOperator'
        self.assertEqual('scan_only', self.classify({'job': job}))
        del job['graph']['nodes'][1]['factory']
        self.assertEqual('unclassified', self.classify({'job': job}))



if __name__ == '__main__':
    unittest.main()
