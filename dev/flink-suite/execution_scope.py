"""Require exact runtime evidence for explicitly selected JUnit fixture variants."""
import json

ROUTES = {'native', 'mixed', 'full_fallback', 'batch_host_only', 'unmodified_plan',
          'scan_only', 'host_failure', 'skipped'}


def validate(audit, path, line):
    scope = json.loads(path.read_text())
    if (not isinstance(scope, dict) or scope.get('schema_version') != 1
            or scope.get('flink_line') != line or line not in ('2.2', '1.18')):
        raise ValueError('Runtime route scope has wrong schema or Flink line')
    requirements = scope.get('cases')
    if not isinstance(requirements, list) or not requirements:
        raise ValueError('Runtime route scope requires nonempty cases')
    seen = set()
    for requirement in requirements:
        if (not isinstance(requirement, dict) or set(requirement) != {'test', 'display_name', 'count', 'route'}
                or not isinstance(requirement['test'], str) or requirement['test'].count('#') != 1
                or not all(requirement['test'].split('#'))
                or not isinstance(requirement['display_name'], str) or not requirement['display_name']
                or type(requirement['count']) is not int or requirement['count'] <= 0
                or not isinstance(requirement['route'], str) or requirement['route'] not in ROUTES):
            raise ValueError('Invalid runtime route requirement')
        key = (requirement['test'], requirement['display_name'])
        if key in seen:
            raise ValueError('Duplicate runtime route requirement')
        seen.add(key)
    report = dict(flink_line=line, requirements=requirements, selected_cases=[])
    audit['scope']['runtime_route_scope'] = report
    problems = []
    for requirement in requirements:
        matches = [case for case in audit['testcases']
                   if case['test'].split('(', 1)[0].split('[', 1)[0] == requirement['test']
                   and case.get('display_name') == requirement['display_name']]
        label = f"{requirement['test']} [{requirement['display_name']}]"
        if len(matches) != requirement['count']:
            problems.append(f"{label}: expected {requirement['count']} runtime cases, got {len(matches)}")
        for case in matches:
            report['selected_cases'].append({key: case.get(key) for key in (
                'test', 'display_name', 'report', 'case_index', 'invocation_id', 'runtime_route')})
            actual = case.get('runtime_route', 'unclassified')
            if actual != requirement['route']:
                problems.append(f"{label}: expected runtime route {requirement['route']}, got {actual}")
    return problems
