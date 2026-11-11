"""Report public fork CI results without executing contributor code."""

import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


WORKFLOWS = {
    'ci.yml': ('All CI tests', 'Rust tests', 'Java tests (debug native build)'),
    'flink-suite.yml': ('All upstream integration tests',),
}
GUIDE = 'https://datafusion-contrib.github.io/StreamFusion/ci/'
POLL_SECONDS = 120
WATCH_SECONDS = 360
MAX_WATCH_SECONDS = 6 * 60 * 60


def api(method, path, data=None):
    request = Request(
        f'https://api.github.com/{path}',
        data=None if data is None else json.dumps(data).encode(),
        method=method,
        headers={
            'Authorization': f'Bearer {os.environ["GH_TOKEN"]}',
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28',
        },
    )
    with urlopen(request, timeout=30) as response:
        return None if response.status == 204 else json.load(response)


def transient_error(error):
    return not isinstance(error, HTTPError) or error.code in (429, 500, 502, 503, 504)


def pages(path, key=None, *, page_size=100):
    page = 1
    while True:
        values = api('GET', f'{path}{"&" if "?" in path else "?"}per_page={page_size}&page={page}')
        if key is not None:
            values = values[key]
        yield from values
        if len(values) < page_size:
            return
        page += 1


def elapsed(start, end):
    if not start or not end:
        return '—'
    seconds = max(0, int((datetime.fromisoformat(end.replace('Z', '+00:00'))
                          - datetime.fromisoformat(start.replace('Z', '+00:00'))).total_seconds()))
    return f'{seconds // 60}m {seconds % 60:02}s'


def job_summary(repository, run):
    jobs = list(pages(f'repos/{repository}/actions/runs/{run["id"]}/jobs?filter=latest',
                      'jobs', page_size=20))
    now = datetime.now(timezone.utc).isoformat()
    rows = ['\n\nIndividual jobs ([full workflow](' + run['html_url'] + ')):',
            '\nStart delay includes dependency builds and runner queueing.\n',
            '| Job | Status | Start delay | Execution |', '|---|---|---:|---:|']
    for index, job in enumerate(jobs):
        name = str(job['name'])[:200].replace('\\', '\\\\').replace('\n', ' ').replace('\r', ' ')
        for character in ('|', '[', ']', '<', '>', '`', '*', '_'):
            name = name.replace(character, '\\' + character)
        status = job.get('conclusion') or job['status']
        started = job.get('started_at') if job['status'] in ('in_progress', 'completed') else None
        duration = elapsed(started, job.get('completed_at') or now) if started else '—'
        row = (f'| [{name}]({job["html_url"]}) | {status} | '
               f'{elapsed(run.get("created_at"), started)} | {duration} |')
        if sum(len(value.encode()) for value in rows) + len(row.encode()) > 45000:
            rows.append(f'\n{len(jobs) - index} additional jobs are available in the full workflow.')
            break
        rows.append(row)
    if not jobs:
        rows.append('| Jobs have not been published yet | queued | — | — |')
    return '\n'.join(rows)


def payload(pr, workflow):
    head = pr['head']
    fork = head.get('repo')
    result = {
        'status': 'completed',
        'conclusion': 'action_required',
        'output': {
            'title': 'Fork CI is required',
            'summary': f'Push this PR head to a personal fork with Actions enabled. See {GUIDE}',
        },
    }
    if not fork or not fork['fork'] or fork['full_name'] == pr['base']['repo']['full_name']:
        return result
    query = urlencode({'head_sha': head['sha'], 'branch': head['ref'], 'per_page': 100})
    try:
        runs = api('GET', f'repos/{fork["full_name"]}/actions/workflows/{workflow}/runs?{query}')
    except HTTPError as error:
        if error.code != 404:
            raise
        return result
    run = next((run for run in runs['workflow_runs']
                if run['head_sha'] == head['sha'] and run['head_branch'] == head['ref']
                and run['event'] in ('push', 'workflow_dispatch')
                # An inventory-only dispatch does not execute the complete upstream suite.
                and (workflow != 'flink-suite.yml' or run['event'] == 'push')), None)
    if run is None:
        return result
    result['details_url'] = run['html_url']
    result['output'] = {
        'title': f'{workflow}: {run["conclusion"] or run["status"]}',
        'summary': (f'[{fork["full_name"]} fork run]({run["html_url"]}) for `{head["sha"]}`. '
                    'Active PR checks are watched until completion; scheduled and manual refreshes '
                    'are also available. '
                    f'See {GUIDE}'),
    }
    try:
        result['output']['summary'] += job_summary(fork['full_name'], run)
    except (URLError, TimeoutError) as error:
        if not transient_error(error):
            raise
        print(f'Run {run["id"]}: job details unavailable: {error}', file=sys.stderr)
        result['output']['summary'] += ('\n\nIndividual job details are temporarily unavailable. '
                                        'See the linked workflow for jobs and logs.')
    if run['status'] == 'completed':
        # Skipped/neutral runs must never satisfy the required test gates.
        result['conclusion'] = 'success' if run['conclusion'] == 'success' else 'failure'
    else:
        result['status'] = 'in_progress' if run['status'] == 'in_progress' else 'queued'
        del result['conclusion']
    return result


def unchanged(check, result):
    return (all(check.get(key) == result.get(key)
                for key in ('status', 'conclusion', 'details_url'))
            and all(check.get('output', {}).get(key) == value
                    for key, value in result.get('output', {}).items()))


def mirror(repository, pr, *, wait_for_runs=False):
    if pr['state'] != 'open':
        return False
    sha = pr['head']['sha']
    fork = pr['head'].get('repo')
    watch_missing = (wait_for_runs and fork and fork['fork']
                     and fork['full_name'] != repository)
    pending = False
    checks = list(pages(f'repos/{repository}/commits/{sha}/check-runs?filter=all', 'check_runs'))
    for workflow, names in WORKFLOWS.items():
        result = payload(pr, workflow)
        pending |= (result['status'] != 'completed'
                    or bool(watch_missing and result.get('conclusion') == 'action_required'))
        for name in names:
            external_id = f'streamfusion:fork-ci:v1:{pr["number"]}:{workflow}:{name}'
            owned_result = {'external_id': external_id, **result}
            # Ordinary workflow jobs use the same application and check names.
            check = next((check for check in checks
                          if check['name'] == name and check['app']['slug'] == 'github-actions'
                          and check.get('external_id') == external_id), None)
            # Requeuing a completed check can retain its old conclusion in the Checks API.
            if check and check.get('status') == 'completed' and result['status'] != 'completed':
                check = None
            if check:
                if not unchanged(check, owned_result):
                    api('PATCH', f'repos/{repository}/check-runs/{check["id"]}', owned_result)
            else:
                api('POST', f'repos/{repository}/check-runs', {'name': name, 'head_sha': sha, **owned_result})
    print(f'Mirrored PR #{pr["number"]} at {sha}')
    return pending


def dispatch_watch(repository, number, deadline, branch):
    api('POST', f'repos/{repository}/actions/workflows/fork-ci-status.yml/dispatches', {
        'ref': branch,
        'inputs': {'pr_number': str(number), 'watch_until': str(deadline)},
    })
    print(f'Queued follow-up for PR #{number}; deadline {deadline}', flush=True)


def watch(repository, number, deadline, branch):
    started = time.monotonic()
    startup_grace = time.time() + POLL_SECONDS
    while True:
        # Fetch the current head every time; a new push must never inherit an old head's result.
        try:
            pr = api('GET', f'repos/{repository}/pulls/{number}')
            if not mirror(repository, pr, wait_for_runs=time.time() < startup_grace):
                return
        except (URLError, TimeoutError) as error:
            if not transient_error(error):
                raise
            print(f'PR #{number}: temporary refresh failure: {error}; retrying', file=sys.stderr)
        remaining = deadline - time.time()
        if remaining <= 0:
            raise SystemExit(f'PR #{number}: fork CI is still pending after the watch deadline; '
                             'refresh manually or inspect the fork runs. Required checks remain pending.')
        if time.monotonic() - started + POLL_SECONDS >= WATCH_SECONDS:
            dispatch_watch(repository, number, deadline, branch)
            return
        time.sleep(min(POLL_SECONDS, remaining))


def refresh(repository, numbers, deadline, branch):
    errors = []
    for number in numbers:
        try:
            if mirror(repository, api('GET', f'repos/{repository}/pulls/{number}')):
                dispatch_watch(repository, number, deadline, branch)
        except Exception as error:
            location = f' ({error.filename})' if isinstance(error, HTTPError) else ''
            message = f'PR #{number}: {error}{location}'
            print(message, file=sys.stderr)
            errors.append(message)
    if errors:
        summary = 'Fork CI refresh failed:\n' + '\n'.join(f'- {error}' for error in errors)
        if os.environ.get('GITHUB_STEP_SUMMARY'):
            with open(os.environ['GITHUB_STEP_SUMMARY'], 'a') as target:
                target.write(summary + '\n')
        raise SystemExit(summary)


def main():
    repository = os.environ['GITHUB_REPOSITORY']
    if repository != 'datafusion-contrib/StreamFusion':
        return
    with open(os.environ['GITHUB_EVENT_PATH']) as source:
        event = json.load(source)
    inputs = event.get('inputs') or {}
    try:
        number = int(str(event.get('pull_request', {}).get('number') or inputs.get('pr_number') or 0))
        deadline = min(int(inputs.get('watch_until') or time.time() + MAX_WATCH_SECONDS),
                       int(time.time() + MAX_WATCH_SECONDS))
        if number < 0:
            raise ValueError('negative PR number')
    except (ValueError, TypeError) as error:
        raise SystemExit(f'Invalid fork CI watch inputs: {error}') from error
    branch = event.get('repository', {}).get('default_branch', 'main')
    if number:
        watch(repository, number, deadline, branch)
        return
    numbers = [pr['number'] for pr in pages(f'repos/{repository}/pulls?state=open')]
    refresh(repository, numbers, deadline, branch)


if __name__ == '__main__':
    main()
