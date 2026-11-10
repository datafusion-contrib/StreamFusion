"""Exercise fork result selection and required-check reporting with fake GitHub responses."""

import copy
from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('fork_ci_status', ROOT / '.github/scripts/fork-ci-status.py')
STATUS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(STATUS)
PR = {
    'number': 42, 'state': 'open',
    'head': {'sha': 'abc123', 'ref': 'feature', 'repo': {'fork': True, 'full_name': 'contributor/StreamFusion'}},
    'base': {'repo': {'full_name': 'datafusion-contrib/StreamFusion'}},
}
RUN = {'id': 10, 'head_sha': 'abc123', 'head_branch': 'feature', 'event': 'push',
       'status': 'completed', 'conclusion': 'success', 'html_url': 'https://github.com/contributor/StreamFusion/actions/runs/10'}


class ForkCiStatusTest(unittest.TestCase):
    def result(self, runs, workflow='ci.yml', pr=None):
        def response(method, path, data=None):
            return {'jobs': []} if '/jobs?' in path else {'workflow_runs': runs}
        with patch.object(STATUS, 'api', side_effect=response) as api:
            result = STATUS.payload(pr or PR, workflow)
        return result, api

    def test_job_table_exposes_links_queue_delay_execution_and_failures(self):
        run = {**RUN, 'created_at': '2026-11-06T11:30:00Z'}
        jobs = [
            {'name': 'Connectors | formats [JDK 11]', 'status': 'completed', 'conclusion': 'success',
             'started_at': '2026-11-06T13:11:00Z', 'completed_at': '2026-11-06T13:21:05Z',
             'html_url': RUN['html_url'] + '/job/1'},
            {'name': 'Runtime', 'status': 'completed', 'conclusion': 'failure',
             'started_at': '2026-11-06T11:30:00Z', 'completed_at': '2026-11-06T12:45:00Z',
             'html_url': RUN['html_url'] + '/job/2'},
            {'name': 'Queued', 'status': 'queued', 'conclusion': None,
             'started_at': '2026-11-06T11:30:00Z', 'completed_at': None,
             'html_url': RUN['html_url'] + '/job/3'},
        ]
        with patch.object(STATUS, 'pages', return_value=iter(jobs)):
            summary = STATUS.job_summary('contributor/StreamFusion', run)
        self.assertIn('101m 00s | 10m 05s', summary)
        self.assertIn('0m 00s | 75m 00s', summary)
        self.assertIn('failure', summary)
        self.assertIn('Connectors \\| formats \\[JDK 11\\]', summary)
        self.assertIn('/job/1', summary)
        self.assertIn('| queued | — | — |', summary)

    def test_job_details_do_not_turn_an_active_workflow_green(self):
        with patch.object(STATUS, 'api', return_value={'workflow_runs': [
                {**RUN, 'status': 'in_progress', 'conclusion': None}]}), \
                patch.object(STATUS, 'job_summary', return_value='Individual jobs: success'):
            result = STATUS.payload(PR, 'ci.yml')
        self.assertEqual('in_progress', result['status'])
        self.assertNotIn('conclusion', result)

    def test_large_job_table_reads_smaller_pages(self):
        job = {'name': 'Runtime', 'status': 'completed', 'conclusion': 'success',
               'html_url': RUN['html_url'] + '/job/1'}
        last = {**job, 'name': 'All CI tests', 'html_url': RUN['html_url'] + '/job/21'}
        with patch.object(STATUS, 'api', side_effect=[{'jobs': [job] * 20}, {'jobs': [last]}]) as api:
            summary = STATUS.job_summary('contributor/StreamFusion', RUN)
        self.assertIn('&per_page=20&page=1', api.call_args_list[0].args[1])
        self.assertIn('&per_page=20&page=2', api.call_args_list[1].args[1])
        self.assertIn('/job/21', summary)

    def test_job_details_outage_preserves_workflow_result(self):
        for status, conclusion in (('completed', 'success'), ('completed', 'failure'),
                                   ('in_progress', None)):
            with self.subTest(status=status, conclusion=conclusion):
                def response(method, path, data=None):
                    if '/jobs?' in path:
                        raise HTTPError(path, 502, 'Bad Gateway', {}, None)
                    return {'workflow_runs': [{**RUN, 'status': status, 'conclusion': conclusion}]}

                with patch.object(STATUS, 'api', side_effect=response):
                    result = STATUS.payload(PR, 'ci.yml')
                self.assertEqual(status, result['status'])
                self.assertEqual(conclusion, result.get('conclusion'))
                self.assertEqual(RUN['html_url'], result['details_url'])
                self.assertIn('job details are temporarily unavailable', result['output']['summary'])

    def test_exact_head_and_branch_are_required(self):
        for change in ({'head_sha': 'old'}, {'head_branch': 'other'}, {'event': 'pull_request'}):
            with self.subTest(change=change):
                result, _ = self.result([{**RUN, **change}])
                self.assertEqual('action_required', result['conclusion'])
        result, _ = self.result([{**RUN, 'head_sha': 'old'}, RUN])
        self.assertEqual('success', result['conclusion'])
        self.assertEqual(RUN['html_url'], result['details_url'])

    def test_failure_cancel_and_skips_cannot_pass(self):
        for conclusion in ('failure', 'cancelled', 'skipped', 'neutral', 'timed_out', 'startup_failure', None):
            with self.subTest(conclusion=conclusion):
                result, _ = self.result([{**RUN, 'conclusion': conclusion}])
                self.assertEqual('failure', result['conclusion'])

    def test_active_runs_remain_pending(self):
        for state in ('queued', 'waiting', 'in_progress'):
            result, _ = self.result([{**RUN, 'status': state, 'conclusion': None}])
            self.assertEqual('in_progress' if state == 'in_progress' else 'queued', result['status'])
            self.assertNotIn('conclusion', result)

    def test_newest_run_wins_including_failed_reruns(self):
        result, _ = self.result([{**RUN, 'conclusion': 'failure'}, RUN])
        self.assertEqual('failure', result['conclusion'])

    def test_upstream_inventory_dispatch_cannot_satisfy_full_suite(self):
        for title in (None, 'Upstream Flink suite', 'SQL admission inventory (runtime)',
                      'SQL admission inventory (all)'):
            with self.subTest(title=title):
                result, _ = self.result([{**RUN, 'event': 'workflow_dispatch',
                                          'display_title': title}], 'flink-suite.yml')
                self.assertEqual('action_required', result['conclusion'])
        result, _ = self.result([{**RUN, 'event': 'workflow_dispatch'}])
        self.assertEqual('success', result['conclusion'])

    def test_full_manual_upstream_suite_is_required_on_the_exact_head(self):
        manual = {**RUN, 'event': 'workflow_dispatch',
                  'display_title': 'Full upstream Flink suite (merge gate)'}
        result, _ = self.result([manual], 'flink-suite.yml')
        self.assertEqual('success', result['conclusion'])
        for change in ({'head_sha': 'old'}, {'head_branch': 'other'},
                       {'conclusion': 'cancelled'}, {'conclusion': 'failure'}):
            with self.subTest(change=change):
                result, _ = self.result([{**manual, **change}], 'flink-suite.yml')
                self.assertNotEqual('success', result['conclusion'])
        result, _ = self.result([{**manual, 'status': 'queued', 'conclusion': None}],
                                'flink-suite.yml')
        self.assertEqual('queued', result['status'])
        self.assertNotIn('conclusion', result)

    def test_inventory_does_not_hide_a_full_suite_or_a_newer_failed_full_rerun(self):
        full = {**RUN, 'event': 'workflow_dispatch',
                'display_title': 'Full upstream Flink suite (merge gate)'}
        inventory = {**full, 'display_title': 'SQL admission inventory (all)'}
        result, _ = self.result([inventory, full], 'flink-suite.yml')
        self.assertEqual('success', result['conclusion'])
        result, _ = self.result([inventory, {**full, 'conclusion': 'failure'}, full],
                                'flink-suite.yml')
        self.assertEqual('failure', result['conclusion'])

    def test_deleted_forks_and_same_repository_prs_need_migration(self):
        for fork in (None, {'fork': False, 'full_name': 'contributor/StreamFusion'},
                     {'fork': True, 'full_name': 'datafusion-contrib/StreamFusion'}):
            pr = copy.deepcopy(PR)
            pr['head']['repo'] = fork
            result, api = self.result([], pr=pr)
            self.assertEqual('action_required', result['conclusion'])
            api.assert_not_called()

    def test_disabled_workflow_is_action_required_and_api_failure_is_not_hidden(self):
        for code in (404, 403, 500):
            with patch.object(STATUS, 'api', side_effect=HTTPError('url', code, 'error', {}, None)):
                if code == 404:
                    self.assertEqual('action_required', STATUS.payload(PR, 'ci.yml')['conclusion'])
                else:
                    with self.assertRaises(HTTPError):
                        STATUS.payload(PR, 'ci.yml')

    def test_only_update_reporter_owned_checks_when_names_collide(self):
        marker = 'streamfusion:fork-ci:v1:42:ci.yml:All CI tests'
        checks = [
            {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'github-actions'},
             'external_id': 'ordinary-actions-job-uuid'},
            {'id': 2, 'name': 'Rust tests', 'app': {'slug': 'another-app'}},
            {'id': 3, 'name': 'All CI tests', 'app': {'slug': 'github-actions'},
             'external_id': marker},
        ]
        with patch.object(STATUS, 'api', return_value={'check_runs': checks}) as api, \
                patch.object(STATUS, 'payload', return_value={'status': 'queued'}):
            STATUS.mirror('datafusion-contrib/StreamFusion', PR)
        calls = api.call_args_list
        self.assertEqual(5, len(calls))
        self.assertEqual('PATCH', calls[1].args[0])
        self.assertTrue(calls[1].args[1].endswith('/check-runs/3'))
        self.assertEqual(marker, calls[1].args[2]['external_id'])
        created = [call.args[2] for call in calls if call.args[0] == 'POST']
        self.assertEqual({'Rust tests', 'Java tests (debug native build)', 'All upstream integration tests'},
                         {check['name'] for check in created})
        self.assertTrue(all(check['head_sha'] == PR['head']['sha'] for check in created))
        self.assertTrue(all(check['external_id'].startswith('streamfusion:fork-ci:v1:42:') for check in created))

    def test_unmarked_or_other_pr_checks_are_not_adopted(self):
        for external_id in (None, '', 'ordinary-actions-job-uuid',
                            'streamfusion:fork-ci:v1:99:ci.yml:All CI tests'):
            with self.subTest(external_id=external_id):
                check = {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'github-actions'},
                         'external_id': external_id}
                with patch.object(STATUS, 'api', return_value={'check_runs': [check]}) as api, \
                        patch.object(STATUS, 'payload', return_value={'status': 'queued'}):
                    STATUS.mirror('datafusion-contrib/StreamFusion', PR)
                self.assertFalse(any(call.args[0] == 'PATCH' for call in api.call_args_list))
                self.assertEqual(4, sum(call.args[0] == 'POST' for call in api.call_args_list))

    def test_owned_marker_from_another_app_is_not_adopted(self):
        check = {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'another-app'},
                 'external_id': 'streamfusion:fork-ci:v1:42:ci.yml:All CI tests'}
        with patch.object(STATUS, 'api', return_value={'check_runs': [check]}) as api, \
                patch.object(STATUS, 'payload', return_value={'status': 'queued'}):
            STATUS.mirror('datafusion-contrib/StreamFusion', PR)
        self.assertFalse(any(call.args[0] == 'PATCH' for call in api.call_args_list))

    def test_unmigrated_pr_remains_action_required_with_legacy_job(self):
        pr = copy.deepcopy(PR)
        pr['head']['repo'] = {**pr['base']['repo'], 'fork': False}
        check = {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'github-actions'}}
        with patch.object(STATUS, 'api', return_value={'check_runs': [check]}) as api:
            STATUS.mirror('datafusion-contrib/StreamFusion', pr)
        created = [call.args[2] for call in api.call_args_list if call.args[0] == 'POST']
        self.assertEqual(4, len(created))
        self.assertTrue(all(check['conclusion'] == 'action_required' for check in created))
        self.assertFalse(any(call.args[0] == 'PATCH' for call in api.call_args_list))

    def test_exact_head_failure_is_mirrored_without_touching_workflow_job(self):
        checks = [
            {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'github-actions'}},
            {'id': 2, 'name': 'All CI tests', 'app': {'slug': 'github-actions'},
             'external_id': 'streamfusion:fork-ci:v1:42:ci.yml:All CI tests'},
        ]
        def response(method, path, data=None):
            if '/check-runs?' in path:
                return {'check_runs': checks}
            if '/actions/workflows/' in path:
                return {'workflow_runs': [{**RUN, 'conclusion': 'failure'}]}
            if '/jobs?' in path:
                return {'jobs': []}
            return {}
        with patch.object(STATUS, 'api', side_effect=response) as api:
            STATUS.mirror('datafusion-contrib/StreamFusion', PR)
        updates = [call for call in api.call_args_list if call.args[0] in ('POST', 'PATCH')]
        self.assertEqual(4, len(updates))
        self.assertTrue(all(call.args[2]['conclusion'] == 'failure' for call in updates))
        self.assertEqual(['repos/datafusion-contrib/StreamFusion/check-runs/2'],
                         [call.args[1] for call in updates if call.args[0] == 'PATCH'])

    def test_owned_check_is_found_beyond_first_page(self):
        legacy = {'id': 1, 'name': 'All CI tests', 'app': {'slug': 'github-actions'}}
        owned = {**legacy, 'id': 2,
                 'external_id': 'streamfusion:fork-ci:v1:42:ci.yml:All CI tests'}
        def response(method, path, data=None):
            if method == 'GET':
                return {'check_runs': [legacy] * 100 if path.endswith('&page=1') else [owned]}
            return {}
        with patch.object(STATUS, 'api', side_effect=response) as api, \
                patch.object(STATUS, 'payload', return_value={'status': 'queued'}):
            STATUS.mirror('datafusion-contrib/StreamFusion', PR)
        self.assertEqual(2, sum(call.args[0] == 'GET' for call in api.call_args_list))
        updated = [call for call in api.call_args_list if call.args[0] == 'PATCH']
        self.assertEqual(1, len(updated))
        self.assertTrue(updated[0].args[1].endswith('/check-runs/2'))
        self.assertIn('filter=all', api.call_args_list[0].args[1])

    def test_closed_pr_is_not_updated(self):
        with patch.object(STATUS, 'api') as api:
            STATUS.mirror('datafusion-contrib/StreamFusion', {**PR, 'state': 'closed'})
        api.assert_not_called()

    def test_schedule_paginates_all_open_prs(self):
        with patch.object(STATUS, 'api', side_effect=[[{'number': n} for n in range(100)], [{'number': 100}]]) as api:
            self.assertEqual(101, len(list(STATUS.pages('repos/owner/repo/pulls?state=open'))))
        self.assertIn('&per_page=100&page=2', api.call_args_list[1].args[1])

    def test_refresh_continues_after_failed_pr_and_preserves_failure(self):
        for fail_fetch in (True, False):
            with self.subTest(fail_fetch=fail_fetch), tempfile.TemporaryDirectory() as directory:
                event = Path(directory) / 'event.json'
                event.write_text('{}')
                summary = Path(directory) / 'summary.md'
                endpoint = 'https://api.github.com/repos/datafusion-contrib/StreamFusion/check-runs/17'
                error = HTTPError(endpoint, 403, 'Forbidden', {}, None)
                def fetch(method, path):
                    number = int(path.rsplit('/', 1)[-1])
                    if fail_fetch and number == 42:
                        raise error
                    return {**PR, 'number': number}
                def mirror(repository, pr):
                    if not fail_fetch and pr['number'] == 42:
                        raise error
                with patch.dict(STATUS.os.environ, {
                    'GITHUB_REPOSITORY': 'datafusion-contrib/StreamFusion',
                    'GITHUB_EVENT_PATH': str(event), 'GITHUB_STEP_SUMMARY': str(summary),
                }), patch.object(STATUS, 'pages', return_value=[{'number': 42}, {'number': 43}]), \
                        patch.object(STATUS, 'api', side_effect=fetch) as api, \
                        patch.object(STATUS, 'mirror', side_effect=mirror) as mirrored:
                    with self.assertRaises(SystemExit) as failure:
                        STATUS.main()
                self.assertEqual([42, 43], [int(call.args[1].rsplit('/', 1)[-1]) for call in api.call_args_list])
                self.assertEqual(43, mirrored.call_args_list[-1].args[1]['number'])
                self.assertIn('PR #42', str(failure.exception))
                self.assertIn(endpoint, summary.read_text())

    def test_pr_event_refreshes_current_head_without_listing_other_prs(self):
        with tempfile.TemporaryDirectory() as directory:
            event = Path(directory) / 'event.json'
            event.write_text(json.dumps({'pull_request': {'number': 42, 'head': {'sha': 'stale'}}}))
            with patch.dict(STATUS.os.environ, {
                'GITHUB_REPOSITORY': 'datafusion-contrib/StreamFusion', 'GITHUB_EVENT_PATH': str(event),
            }), patch.object(STATUS, 'pages') as pages, \
                    patch.object(STATUS, 'api', return_value=PR) as api, \
                    patch.object(STATUS, 'mirror', return_value=False) as mirror:
                STATUS.main()
            pages.assert_not_called()
            api.assert_called_once_with('GET', 'repos/datafusion-contrib/StreamFusion/pulls/42')
            mirror.assert_called_once_with('datafusion-contrib/StreamFusion', PR, wait_for_runs=True)

    def test_unchanged_owned_checks_do_not_generate_repeated_writes(self):
        result = {'status': 'in_progress', 'details_url': RUN['html_url'],
                  'output': {'title': 'running', 'summary': 'waiting'}}
        checks = [
            {'id': index, 'name': name, 'app': {'slug': 'github-actions'},
             'external_id': f'streamfusion:fork-ci:v1:42:{workflow}:{name}', **result}
            for index, (workflow, name) in enumerate(
                (workflow, name) for workflow, names in STATUS.WORKFLOWS.items() for name in names)
        ]
        with patch.object(STATUS, 'api', return_value={'check_runs': checks}) as api, \
                patch.object(STATUS, 'payload', return_value=result):
            self.assertTrue(STATUS.mirror('datafusion-contrib/StreamFusion', PR))
        self.assertEqual(['GET'], [call.args[0] for call in api.call_args_list])

    def test_pending_to_failure_and_output_changes_are_not_skipped(self):
        check = {'status': 'completed', 'conclusion': 'success', 'details_url': RUN['html_url'],
                 'output': {'title': 'success', 'summary': 'old'}}
        self.assertTrue(STATUS.unchanged(check, check))
        for change in ({'status': 'in_progress', 'conclusion': None},
                       {'conclusion': 'failure'}, {'details_url': 'new-run'},
                       {'output': {'title': 'success', 'summary': 'new'}}):
            with self.subTest(change=change):
                self.assertFalse(STATUS.unchanged(check, {**check, **change}))

    def test_completed_checks_start_a_new_attempt_when_the_fork_run_is_active(self):
        for conclusion in ('action_required', 'failure', 'success'):
            for state in ('queued', 'in_progress'):
                with self.subTest(conclusion=conclusion, state=state):
                    result = {'status': state, 'details_url': RUN['html_url'],
                              'output': {'title': state, 'summary': 'waiting'}}
                    checks = [
                        {'id': index, 'name': name, 'app': {'slug': 'github-actions'},
                         'external_id': f'streamfusion:fork-ci:v1:42:{workflow}:{name}',
                         'status': 'completed', 'conclusion': conclusion}
                        for index, (workflow, name) in enumerate(
                            (workflow, name) for workflow, names in STATUS.WORKFLOWS.items()
                            for name in names)
                    ]
                    with patch.object(STATUS, 'api', return_value={'check_runs': checks}) as api, \
                            patch.object(STATUS, 'payload', return_value=result):
                        self.assertTrue(STATUS.mirror('datafusion-contrib/StreamFusion', PR))
                    writes = [call for call in api.call_args_list if call.args[0] != 'GET']
                    self.assertEqual(len(checks), len(writes))
                    active = []
                    for previous, call in zip(checks, writes):
                        self.assertEqual(('POST', 'repos/datafusion-contrib/StreamFusion/check-runs'),
                                         call.args[:2])
                        created = call.args[2]
                        self.assertEqual(PR['head']['sha'], created['head_sha'])
                        self.assertEqual(previous['external_id'], created['external_id'])
                        self.assertEqual(state, created['status'])
                        self.assertNotIn('conclusion', created)
                        active.append({'id': 100 + previous['id'],
                                       'app': {'slug': 'github-actions'}, **created})
                    with patch.object(STATUS, 'api', return_value={'check_runs': active + checks}) as api, \
                            patch.object(STATUS, 'payload', return_value=result):
                        self.assertTrue(STATUS.mirror('datafusion-contrib/StreamFusion', PR))
                    self.assertEqual(['GET'], [call.args[0] for call in api.call_args_list])

    def test_missing_runs_get_a_startup_grace_without_becoming_successful(self):
        result = {'status': 'completed', 'conclusion': 'action_required'}
        with patch.object(STATUS, 'api', return_value={'check_runs': []}) as api, \
                patch.object(STATUS, 'payload', return_value=result):
            self.assertTrue(STATUS.mirror('datafusion-contrib/StreamFusion', PR, wait_for_runs=True))
            self.assertFalse(STATUS.mirror('datafusion-contrib/StreamFusion', PR))
        writes = [call.args[2] for call in api.call_args_list if call.args[0] == 'POST']
        self.assertTrue(all(write['conclusion'] == 'action_required' for write in writes))
        same_repo = copy.deepcopy(PR)
        same_repo['head']['repo'] = {**PR['base']['repo'], 'fork': False}
        with patch.object(STATUS, 'api', return_value={'check_runs': []}), \
                patch.object(STATUS, 'payload', return_value=result):
            self.assertFalse(STATUS.mirror('datafusion-contrib/StreamFusion', same_repo,
                                          wait_for_runs=True))


class ForkCiWatchTest(unittest.TestCase):
    @contextmanager
    def clock(self):
        elapsed = [0]
        with patch.object(STATUS, 'time') as clock:
            clock.time.side_effect = lambda: 1000 + elapsed[0]
            clock.monotonic.side_effect = lambda: elapsed[0]
            clock.sleep.side_effect = lambda seconds: elapsed.__setitem__(0, elapsed[0] + seconds)
            yield clock

    @contextmanager
    def event(self, value):
        with tempfile.TemporaryDirectory() as directory:
            event = Path(directory) / 'event.json'
            event.write_text(json.dumps(value))
            with patch.dict(STATUS.os.environ, {
                'GITHUB_REPOSITORY': 'datafusion-contrib/StreamFusion',
                'GITHUB_EVENT_PATH': str(event),
            }):
                yield

    def test_watch_refreshes_new_heads_until_both_suites_are_terminal(self):
        newer = copy.deepcopy(PR)
        newer['head']['sha'] = 'new-head'
        with self.clock() as clock, \
                patch.object(STATUS, 'api', side_effect=[PR, newer, newer]) as api, \
                patch.object(STATUS, 'mirror', side_effect=[True, True, False]) as mirror, \
                patch.object(STATUS, 'dispatch_watch') as dispatch:
            STATUS.watch('datafusion-contrib/StreamFusion', 42, 10000, 'main')
        self.assertEqual(3, api.call_count)
        self.assertEqual(['abc123', 'new-head', 'new-head'],
                         [call.args[1]['head']['sha'] for call in mirror.call_args_list])
        self.assertEqual([True, False, False],
                         [call.kwargs['wait_for_runs'] for call in mirror.call_args_list])
        self.assertEqual([120, 120], [call.args[0] for call in clock.sleep.call_args_list])
        dispatch.assert_not_called()

    def test_real_mirroring_tracks_independently_finishing_suites(self):
        for conclusion in ('success', 'failure'):
            with self.subTest(conclusion=conclusion):
                checks = []
                poll = [0]

                def response(method, path, data=None):
                    if path.endswith('/pulls/42'):
                        poll[0] += 1
                        return copy.deepcopy(PR)
                    if '/check-runs?' in path:
                        return {'check_runs': copy.deepcopy(checks)}
                    if '/actions/workflows/' in path:
                        complete = poll[0] >= (2 if '/ci.yml/' in path else 3)
                        return {'workflow_runs': [{
                            **RUN, 'status': 'completed' if complete else 'in_progress',
                            'conclusion': conclusion if complete else None,
                        }]}
                    if '/jobs?' in path:
                        return {'jobs': []}
                    if method == 'POST':
                        checks.append({'id': len(checks) + 1,
                                       'app': {'slug': 'github-actions'}, **copy.deepcopy(data)})
                        return copy.deepcopy(checks[-1])
                    if method == 'PATCH':
                        check = checks[int(path.rsplit('/', 1)[-1]) - 1]
                        check.update(copy.deepcopy(data))
                        return copy.deepcopy(check)
                    self.fail(f'Unexpected API request: {method} {path}')

                with self.clock() as clock, patch.object(STATUS, 'api', side_effect=response) as api, \
                        patch.object(STATUS, 'dispatch_watch') as dispatch:
                    STATUS.watch('datafusion-contrib/StreamFusion', 42, 10000, 'main')
                self.assertEqual(3, poll[0])
                self.assertEqual([120, 120], [call.args[0] for call in clock.sleep.call_args_list])
                self.assertEqual(4, len(checks))
                self.assertTrue(all(check['status'] == 'completed'
                                    and check['conclusion'] == conclusion for check in checks))
                self.assertTrue(all(check['head_sha'] == PR['head']['sha'] for check in checks))
                self.assertEqual(4, sum(call.args[0] == 'PATCH' for call in api.call_args_list))
                dispatch.assert_not_called()

    def test_long_running_suites_continue_without_extending_the_deadline(self):
        with self.clock() as clock, patch.object(STATUS, 'api', return_value=PR), \
                patch.object(STATUS, 'mirror', return_value=True), \
                patch.object(STATUS, 'dispatch_watch') as dispatch:
            STATUS.watch('datafusion-contrib/StreamFusion', 42, 10000, 'trusted-default')
        dispatch.assert_called_once_with('datafusion-contrib/StreamFusion', 42, 10000,
                                         'trusted-default')
        self.assertEqual([120, 120], [call.args[0] for call in clock.sleep.call_args_list])

    def test_watch_deadline_expires_without_passing_or_restarting_tests(self):
        with self.clock() as clock, patch.object(STATUS, 'api', return_value=PR), \
                patch.object(STATUS, 'mirror', return_value=True), \
                patch.object(STATUS, 'dispatch_watch') as dispatch:
            with self.assertRaisesRegex(SystemExit, 'Required checks remain pending'):
                STATUS.watch('datafusion-contrib/StreamFusion', 42, 1110, 'main')
        dispatch.assert_not_called()
        self.assertEqual([110], [call.args[0] for call in clock.sleep.call_args_list])

    def test_transient_api_failure_does_not_abandon_the_watch(self):
        for error in (URLError('connection reset'), TimeoutError('timeout'),
                      HTTPError('url', 503, 'Unavailable', {}, None),
                      HTTPError('url', 429, 'Too Many Requests', {}, None)):
            with self.subTest(error=error), self.clock() as clock, \
                    patch.object(STATUS, 'api', side_effect=[error, PR]), \
                    patch.object(STATUS, 'mirror', return_value=False), \
                    patch.object(STATUS, 'dispatch_watch') as dispatch:
                STATUS.watch('datafusion-contrib/StreamFusion', 42, 10000, 'main')
                clock.sleep.assert_called_once_with(120)
                dispatch.assert_not_called()

    def test_permanent_permission_failures_are_not_retried_or_hidden(self):
        with self.clock() as clock, patch.object(STATUS, 'api', side_effect=
                HTTPError('url', 403, 'Forbidden', {}, None)), \
                patch.object(STATUS, 'dispatch_watch') as dispatch:
            with self.assertRaises(HTTPError):
                STATUS.watch('datafusion-contrib/StreamFusion', 42, 10000, 'main')
        clock.sleep.assert_not_called()
        dispatch.assert_not_called()

    def test_closed_pr_and_completed_failure_stop_the_watch(self):
        for pr in ({**PR, 'state': 'closed'}, PR):
            with self.subTest(state=pr['state']), self.clock() as clock, \
                    patch.object(STATUS, 'api', return_value=pr), \
                    patch.object(STATUS, 'payload', return_value={
                        'status': 'completed', 'conclusion': 'failure'}), \
                    patch.object(STATUS, 'pages', return_value=[]), \
                    patch.object(STATUS, 'dispatch_watch') as dispatch:
                STATUS.watch('datafusion-contrib/StreamFusion', 42, 10000, 'main')
                clock.sleep.assert_not_called()
                dispatch.assert_not_called()

    def test_scheduled_refresh_starts_only_pending_pr_watches(self):
        with self.event({}), self.clock(), \
                patch.object(STATUS, 'pages', return_value=[{'number': 42}, {'number': 43}]), \
                patch.object(STATUS, 'api', side_effect=lambda method, path: {
                    **PR, 'number': int(path.rsplit('/', 1)[-1])}), \
                patch.object(STATUS, 'mirror', side_effect=lambda repo, pr: pr['number'] == 42), \
                patch.object(STATUS, 'dispatch_watch') as dispatch:
            STATUS.main()
        dispatch.assert_called_once_with('datafusion-contrib/StreamFusion', 42, 22600, 'main')

    def test_one_failed_refresh_does_not_prevent_other_pr_watches(self):
        def fetch(method, path):
            if path.endswith('/42'):
                raise HTTPError(path, 403, 'Forbidden', {}, None)
            return {**PR, 'number': 43}
        with self.event({}), self.clock(), \
                patch.object(STATUS, 'pages', return_value=[{'number': 42}, {'number': 43}]), \
                patch.object(STATUS, 'api', side_effect=fetch), \
                patch.object(STATUS, 'mirror', return_value=True), \
                patch.object(STATUS, 'dispatch_watch') as dispatch:
            with self.assertRaisesRegex(SystemExit, 'PR #42'):
                STATUS.main()
        dispatch.assert_called_once_with('datafusion-contrib/StreamFusion', 43, 22600, 'main')

    def test_dispatch_uses_only_the_default_branch_and_preserves_the_deadline(self):
        with patch.object(STATUS, 'api') as api:
            STATUS.dispatch_watch('datafusion-contrib/StreamFusion', 42, 10000, 'main')
        api.assert_called_once_with('POST',
            'repos/datafusion-contrib/StreamFusion/actions/workflows/fork-ci-status.yml/dispatches',
            {'ref': 'main', 'inputs': {'pr_number': '42', 'watch_until': '10000'}})

    def test_manual_continuation_is_scoped_and_cannot_extend_past_six_hours(self):
        for value, deadline in (('5000', 5000), ('999999999', 22600), ('', 22600)):
            with self.subTest(value=value), self.event({
                'inputs': {'pr_number': '42', 'watch_until': value},
                'repository': {'default_branch': 'trusted-default'},
            }), self.clock(), patch.object(STATUS, 'watch') as watch, \
                    patch.object(STATUS, 'pages') as pages:
                STATUS.main()
                watch.assert_called_once_with('datafusion-contrib/StreamFusion', 42, deadline,
                                              'trusted-default')
                pages.assert_not_called()

    def test_invalid_dispatch_inputs_do_not_make_requests(self):
        for inputs in ({'pr_number': '-1'}, {'pr_number': '42; evil'}, {'pr_number': 42.5},
                       {'pr_number': '42', 'watch_until': 'not-a-time'}):
            with self.subTest(inputs=inputs), self.event({'inputs': inputs}), \
                    patch.object(STATUS, 'api') as api:
                with self.assertRaisesRegex(SystemExit, 'Invalid fork CI watch inputs'):
                    STATUS.main()
                api.assert_not_called()

    def test_workflow_dispatch_accepts_githubs_empty_response(self):
        with patch.dict(STATUS.os.environ, {'GH_TOKEN': 'test-only'}), \
                patch.object(STATUS, 'urlopen') as urlopen:
            response = urlopen.return_value.__enter__.return_value
            response.status = 204
            self.assertIsNone(STATUS.api('POST', 'repos/owner/repo/dispatches', {}))
            response.read.assert_not_called()


if __name__ == '__main__':
    unittest.main()
