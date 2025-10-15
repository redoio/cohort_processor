# -*- coding: utf-8 -*-
from tqdm import tqdm
from .cohort_processor import CohortGenerator
from . import config
from pandas import ExcelWriter


ruleset = {'criteria': {'controlling_offense': {'Controlling Offense': {'types': ['Serious felonies', 'Super strike offenses', 'Violent felonies', 'Registrable sex offenses'],
                                                                         'mode': 'Exclude', 
                                                                         'data_label': 'demographics',
                                                                         'implications': {'codes': {'all': ["/att", "(664)", "2nd"], 
                                                                                                   '459': ["/att", "(664)"]}, 
                                                                                          'perm': 2}}},
                        'sentence_length': {'Aggregate Sentence in Months': {'min': 300, 
                                                                             'max': 10000000, 
                                                                             'data_label': 'demographics'}},
                        'prior_commitments': {'Offense': {'types': ['Super strike offenses'],
                                                          'mode': 'Exclude', 
                                                          'data_label': 'prior_commitments',
                                                          'implications': {'codes': {'all': ["/att", "(664)", "2nd"], 
                                                                                    '459': ["/att", "(664)"]}, 
                                                                           'perm': 2}}},
                        'current_commitments': {'Offense': {'types': ['Super strike offenses'],
                                                             'mode': 'Exclude', 
                                                             'data_label': 'current_commitments',
                                                             'implications': {'codes': {'all': ["/att", "(664)", "2nd"], 
                                                                                       '459': ["/att", "(664)"]}, 
                                                                              'perm': 2}}}}}


cohort = CohortGenerator(label = 'non-non-nons', desc = "Trial")

cohort.get_raw_data(input_data_path = {'demographics': config.RAW_DEMOGRAPHICS_URL, 
                                        'current_commitments': config.RAW_CURRENT_COMMITMENTS_URL, 
                                        'prior_commitments': config.RAW_PRIOR_COMMITMENTS_URL}, 
                    id_var = "CDCNo", 
                    clean_col_names = True)

cohort.get_offense_categorizations(config.OFFENSE_CODES_URL)
cohort.get_ruleset(ruleset = ruleset)
cohort.apply_ruleset(prefix = "PC", clean_col_names = True, pop_ids = 'demographics_raw', use_t_cols = ["aggregate sentence in months", "offense end date"])

qual_cohort_current_commits = cohort.current_commitments_raw[~cohort.current_commitments_raw[cohort.id].isin(cohort.disqual_ids)]
qual_cohort_prior_commits = cohort.prior_commitments_raw[~cohort.prior_commitments_raw[cohort.id].isin(cohort.disqual_ids)]
qual_cohort_demographics = cohort.demographics_raw[~cohort.demographics_raw[cohort.id].isin(cohort.disqual_ids)]

enh_qual_ids = []
for id_val, gp in tqdm(qual_cohort_current_commits.groupby(cohort.id)):
    current_offenses = gp[['offense', 'off_enh1', 'off_enh2', 'off_enh3', 'off_enh4']].values.flatten()
    if 'PC667.5(b)' in current_offenses:
        enh_qual_ids.append(id_val)

# Selection based on enhancements
qual_cohort_current_commits_enh = cohort.current_commitments_raw[cohort.current_commitments_raw[cohort.id].isin(enh_qual_ids)]
qual_cohort_prior_commits_enh = cohort.prior_commitments_raw[cohort.prior_commitments_raw[cohort.id].isin(enh_qual_ids)]
qual_cohort_demographics_enh = cohort.demographics_raw[cohort.demographics_raw[cohort.id].isin(enh_qual_ids)]
    
# Write output
with ExcelWriter("output.xlsx") as writer:
    qual_cohort_current_commits.to_excel(writer, sheet_name='Current Commits', startrow = 6)
    qual_cohort_prior_commits.to_excel(writer, sheet_name='Prior Commits', startrow = 6)
    qual_cohort_demographics.to_excel(writer, sheet_name='Demographics', startrow = 6)
    qual_cohort_current_commits_enh.to_excel(writer, sheet_name='Current Commits (Enh)', startrow = 6)
    qual_cohort_prior_commits_enh.to_excel(writer, sheet_name='Prior Commits (Enh)', startrow = 6)
    qual_cohort_demographics_enh.to_excel(writer, sheet_name='Demographics (Enh)', startrow = 6)
