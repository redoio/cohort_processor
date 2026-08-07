import unittest

import pandas as pd

from cohort_processor import CohortGenerator
from cohort_processor import utils


class EnhancementRuleTests(unittest.TestCase):
    def setUp(self):
        self.cohort = CohortGenerator()
        self.cohort.id = "cdcno"
        self.cohort.disqual_ids = []
        self.cohort.demographics_raw = pd.DataFrame(
            {"cdcno": ["A", "B", "C", "D"]}
        )
        self.cohort.current_commitments_raw = pd.DataFrame(
            {
                "cdcno": ["A", "B", "C", "C", "D"],
                "off enh1": [
                    "PC12022.5",
                    "PC999",
                    None,
                    "PC12022.5",
                    "PC12022.5",
                ],
                "off enh2": [
                    None,
                    None,
                    None,
                    "PC186.22",
                    "PC12022.5",
                ],
            }
        )

    def apply_rule(self, how):
        return self.cohort.apply_enhancement_rules(
            data="current_commitments_raw",
            sel_enh=["12022.5", "186.22"],
            how=how,
            prefix="PC",
            enh_var=["off enh1", "off enh2"],
            pop_ids="demographics_raw",
        )

    def test_exclude_requires_two_distinct_matching_enhancements(self):
        disqualified = self.apply_rule("Exclude")

        self.assertEqual({"C"}, set(disqualified))

    def test_include_disqualifies_only_ids_without_any_match(self):
        disqualified = self.apply_rule("Include")

        self.assertEqual({"B"}, set(disqualified))

    def test_duplicate_matches_on_one_record_count_as_one_enhancement(self):
        disqualified = self.apply_rule("Exclude")

        self.assertNotIn("D", disqualified)

    def test_existing_disqualifications_are_preserved_and_not_reprocessed(self):
        self.cohort.disqual_ids = ["A"]

        disqualified = self.apply_rule("Exclude")

        self.assertEqual({"A", "C"}, set(disqualified))

    def test_empty_selection_does_not_change_results(self):
        disqualified = self.cohort.apply_enhancement_rules(
            data="current_commitments_raw",
            sel_enh=[],
            how="Exclude",
            prefix="PC",
            enh_var=["off enh1", "off enh2"],
            pop_ids="demographics_raw",
        )

        self.assertEqual([], disqualified)

    def test_empty_include_selection_disqualifies_all_ids(self):
        disqualified = self.cohort.apply_enhancement_rules(
            data="current_commitments_raw",
            sel_enh=[],
            how="Include",
            prefix="PC",
            enh_var=["off enh1", "off enh2"],
            pop_ids="demographics_raw",
        )

        self.assertEqual({"A", "B", "C", "D"}, set(disqualified))

    def test_unknown_selection_logic_does_not_change_results(self):
        disqualified = self.cohort.apply_enhancement_rules(
            data="current_commitments_raw",
            sel_enh=["12022.5"],
            how="Unknown",
            prefix="PC",
            enh_var=["off enh1", "off enh2"],
            pop_ids="demographics_raw",
        )

        self.assertEqual([], disqualified)

    def test_series_cleaning_preserves_original_missing_value_behavior(self):
        values = pd.Series(["PC 12022.5.", None])

        cleaned = utils.clean_blk(values)

        self.assertEqual("12022.5", cleaned.iloc[0])
        self.assertEqual("none", cleaned.iloc[1])


if __name__ == "__main__":
    unittest.main()
