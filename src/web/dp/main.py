import sys

from backend.mock.driver_test import run_test_tests
from run import run_all_tests, print_test_results

import log


def test_sb_data_pipeline():
    test_results = run_all_tests()
    log.i('All Tests Complete')
    log.d('Results:')
    log.d(test_results)
    passed_dict = {r['name']: (r.get('results', None) or {}).get('passed', False) for r in test_results}
    passed = all(passed_dict.values())
    test_count = len(test_results)
    passed_count = sum(1 for r in passed_dict.values() if r)

    print_test_results(test_results)

    if passed:
        log.i('%s of %s TESTS PASSED!!' % (passed_count, test_count))
        quit(0)
    else:
        log.i('%s of %s TESTS FAILED!!' % (test_count - passed_count, test_count))
        quit(1)


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '-test':
        run_test_tests()
    else:
        test_sb_data_pipeline()

