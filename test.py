# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
from cohort_processor import CohortGenerator
import config
import json
import pandas as pd

ruleset = {'criteria': {'sentence_length': {'aggregate sentence in months': {'min': 0, 
                                                                             'max': 10000000, 
                                                                             'data_label': 'demographics'}},
                        'sentence_served': {'time served in years': {'min': 0, 
                                                                     'max': 50, 
                                                                     'data_label': 'demographics'}},
                        'prior_commitments': {'offense': {'types': ['Serious felonies'],
                                                          'mode': 'Include', 
                                                          'data_label': 'prior_commitments',
                                                          'implications': {'codes': {'all': ["/att", "(664)", "2nd"], 
                                                                                    '459': ["/att", "(664)"]}, 
                                                                           'perm': 2}}}}}


# Initialize the cohort and generate a non-non-nons scenario
cohort = CohortGenerator(label = 'non-non-nons', desc = "Trial")
cohort.get_raw_data(input_data_path = {'demographics': config.DEFAULT_DATA_URL, 
                                       'current_commitments': config.CURRENT_COMMITMENTS_URL, 
                                       'prior_commitments': config.PRIOR_COMMITMENTS_URL}, 
                    id_var = "cdcno", 
                    clean_col_names = True)
cohort.get_offense_categorizations(config.OFFENSE_CODES_URL)
cohort.get_ruleset(ruleset = ruleset)
cohort.apply_ruleset(prefix = "PC", clean_col_names = True, pop_ids = 'demographics_raw', use_t_cols = ["aggregate sentence in months", "offense end date"])
cohort.get_responsive_data(input_data_path = {'demographics': config.DEFAULT_DATA_URL, 
                                              'current_commitments': config.CURRENT_COMMITMENTS_URL, 
                                              'prior_commitments': config.PRIOR_COMMITMENTS_URL})

# See the resultant records 
cohort.demographics 
cohort.current_commitments
cohort.prior_commitments
