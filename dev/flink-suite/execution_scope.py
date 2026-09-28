"""Require exact runtime evidence for explicitly selected JUnit fixture variants."""
import json

ROUTES = {'native', 'mixed', 'full_fallback', 'batch_host_only', 'unmodified_plan',
          'scan_only', 'host_failure', 'skipped'}


def validate(audit, path, line):
    scope = json.loads(path.read_text())
    if (not isinstance(scope, dict) or type(scope.get('schema_version')) is not int
            or scope['schema_version'] not in (1, 2)
            or scope.get('flink_line') != line or line not in ('2.2', '1.18')):
        raise ValueError('Runtime route scope has wrong schema or Flink line')
    requirements = scope.get('cases')
    if not isinstance(requirements, list) or not requirements:
        raise ValueError('Runtime route scope requires nonempty cases')
    seen = set()
    for requirement in requirements:
        base = {'test', 'display_name', 'count'}
        if (not isinstance(requirement, dict)
                or set(requirement) not in (base | {'route'}, base | {'route_by_contract_variant'})
                or not isinstance(requirement['test'], str) or requirement['test'].count('#') != 1
                or not all(requirement['test'].split('#'))
                or not isinstance(requirement['display_name'], str) or not requirement['display_name']
                or type(requirement['count']) is not int or requirement['count'] <= 0):
            raise ValueError('Invalid runtime route requirement')
        if 'route' in requirement:
            valid = isinstance(requirement['route'], str) and requirement['route'] in ROUTES
        else:
            if scope['schema_version'] < 2:
                raise ValueError('Conditional runtime routes require scope schema version 2')
            variants = requirement['route_by_contract_variant']
            valid = (isinstance(variants, dict) and bool(variants)
                     and all(isinstance(variant, str) and variant
                             and isinstance(route, str) and route in ROUTES
                             for variant, route in variants.items()))
        if not valid:
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
                'test', 'display_name', 'report', 'case_index', 'invocation_id', 'runtime_route',
                'execution_contract_variant')
                if key != 'execution_contract_variant' or key in case})
            actual = case.get('runtime_route', 'unclassified')
            expected = requirement.get('route')
            if 'route_by_contract_variant' in requirement:
                variant = case.get('execution_contract_variant')
                expected = requirement['route_by_contract_variant'].get(variant)
                if expected is None:
                    problems.append(f"{label}: no runtime route declared for contract variant {variant!r}")
                    continue
            if actual != expected:
                problems.append(f"{label}: expected runtime route {expected}, got {actual}")
    return problems
