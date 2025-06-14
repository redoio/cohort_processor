# -*- coding: utf-8 -*-
from cohort_processor import CohortGenerator
import config
import json
import pd

ruleset = {'criteria': {
#                       {'controlling_offense': {'Controlling Offense': {'types': ['Serious felonies', 'Super strike offenses', 'Violent felonies', 'Registrable sex offenses'],
#                                                                          'mode': 'Exclude', 
#                                                                          'data_label': 'demographics',
#                                                                          'implications': {'codes': {'all': ["/att", "(664)", "2nd"], 
#                                                                                                    '459': ["/att", "(664)"]}, 
#                                                                                           'perm': 2}}},
                        'sentence_length': {'Aggregate Sentence in Months': {'min': 240, 
                                                                             'max': 10000000, 
                                                                             'data_label': 'demographics'}},
                        'sentence_served': {'time served in years': {'min': 10, 
                                                                     'max': 10000000, 
                                                                     'data_label': 'demographics'}},
                        'prior_commitments': {'Offense': {'types': ['Super strike offenses', 'Registrable sex offenses'],
                                                          'mode': 'Exclude', 
                                                          'data_label': 'prior_commitments',
                                                          'implications': {'codes': {'all': ["/att", "(664)", "2nd"], 
                                                                                    '459': ["/att", "(664)"]}, 
                                                                           'perm': 2}}},
                        'current_commitments': {'Offense': {'types': ['Serious felonies', 'Super strike offenses', 'Violent felonies', 'Registrable sex offenses'],
                                                             'mode': 'Exclude', 
                                                             'data_label': 'current_commitments',
                                                             'implications': {'codes': {'all': ["/att", "(664)", "2nd"], 
                                                                                       '459': ["/att", "(664)"]}, 
                                                                              'perm': 2}}}}}


# Initialize the cohort and generate a non-non-nons scenario
cohort = CohortGenerator(label = 'non-non-nons', desc = "Trial")
cohort.get_raw_data(input_data_path = {'demographics': config.RAW_DEMOGRAPHICS_URL, 
                                       'current_commitments': config.RAW_CURRENT_COMMITMENTS_URL, 
                                       'prior_commitments': config.RAW_PRIOR_COMMITMENTS_URL}, 
                    id_var = "CDCNo", 
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

# Get ID map 
with open(config.HASH_OBJ_MAP, 'r') as file: 
    map_ids = json.load(file)
    
# Get IDs in cohort not in another cohort 
diff_ids = []
demographics_nnn = pd.read_excel(config.NNN_DATA_URL)
for id_val in cohort.demographics['cdcno']:
    if map_ids[id_val] not in list(demographics_nnn['cdcno']):
        diff_ids.append(id_val)
        
cohort.demographics[cohort.demographics.isin(diff_ids)].to_excel('test.xlsx')
        