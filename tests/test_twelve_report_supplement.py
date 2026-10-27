"""Small reporting fixtures; these are not database measurements."""
import unittest
import numpy as np

from scripts.supplement_twelve_report import cohort_rows


class CohortReportTests(unittest.TestCase):
    def setUp(self):
        self.values = {k: np.array([1., .5, .25, .75]) for k in (10, 100)}
        self.names = ['old', 'changed', 'empty']
        self.reported = {'old': {'recall10': .75, 'recall100': .75},
                         'changed': {'recall10': .5, 'recall100': .5}}

    def test_counts_pairing_and_empty_cohort(self):
        rebuilt = {k: np.stack([v, v, v]) for k, v in self.values.items()}
        rows = cohort_rows([0, 0, 1, 1], self.names, self.values, self.reported, 'fixture', rebuilt)
        self.assertEqual([v['queries'] for v in rows], [2, 2, 0])
        self.assertEqual(rows[0]['fresh_delta100']['mean_pp'], 0.)
        self.assertEqual(rows[0]['fresh_delta100']['queries'], 2)
        self.assertEqual(rows[1]['fresh_min100'], .5)
        self.assertIsNone(rows[2]['recall100'])
        self.assertIsNone(rows[2]['fresh_delta100'])

    def test_wrong_labels_fail_against_measured_groups(self):
        with self.assertRaisesRegex(AssertionError, 'scores differ'):
            cohort_rows([1, 1, 0, 0], self.names, self.values, self.reported, 'mutated-labels')

    def test_missing_query_and_invalid_code_fail(self):
        for codes in ([0, 0, 1], [0, 0, 1, 9]):
            with self.assertRaisesRegex(AssertionError, 'alignment'):
                cohort_rows(codes, self.names, self.values, self.reported, 'mutated-coverage')

    def test_missing_reported_cohort_fails(self):
        with self.assertRaisesRegex(AssertionError, 'membership'):
            cohort_rows([0, 0, 1, 1], self.names, self.values, {'old': self.reported['old']}, 'missing-group')


if __name__ == '__main__':
    unittest.main()
