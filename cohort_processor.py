# -*- coding: utf-8 -*-
import pandas as pd
from tqdm import tqdm
import datetime
import utils
from dateutil.relativedelta import relativedelta
import impl
import traceback
import sys
import os

class CohortGenerator():
    def __init__(self, label = "", desc = ""):
        self.label = label
        self.desc = desc
        # Add cache for loaded data to improve speed
        self._data_cache = {}
        
    def get_raw_data(self, input_data_path : dict, id_var : str, clean_col_names : bool):
        # Load all data and clean column names if required
        for cat in input_data_path.keys():
            # Check if data is already cached to avoid reloading
            cache_key = f"{cat}_{input_data_path[cat]}"
            
            if cache_key in self._data_cache:
                setattr(self, cat+"_raw", self._data_cache[cache_key].copy())
            else:
                data = utils.load_data(input_data_path[cat])
                setattr(self, cat+"_raw", data)
                self._data_cache[cache_key] = data.copy()  # Cache the data
                print('Loaded and cached raw data in path: ', input_data_path[cat])
                
        print("\n")
        
        # Clean col names if requested 
        if clean_col_names: 
            for cat in input_data_path.keys(): 
                df = getattr(self, cat+"_raw")
                df.columns = utils.clean_blk(list(df.columns), remove = ['rape', '\n'])
                print(f"For dataset {cat}, cleaned columns and set new values: {df.columns}")
                setattr(self, cat+"_raw", df)
        print("\n")   
        
        # Clean ID variable 
        id_var = utils.clean(id_var, remove = None)
        
        # Check if all input datasets have the same ID variable
        truth_count = 0
        for cat in input_data_path.keys():
            if id_var in getattr(self, cat+"_raw"):
                truth_count += 1
            else:
                print("ID not available in dataset: ", cat)
        # Assign ID variable to class if shared across all inputs 
        if truth_count == len(input_data_path.keys()):
            self.id = id_var
        else: 
            self.id = None
        return 
    
    def get_offense_categorizations(self, categories_data_path : str):
        if 'csv' in categories_data_path:
            self.offense_categories = pd.read_csv(categories_data_path)
        if 'xlsx' in categories_data_path:
            self.offense_categories = pd.read_excel(categories_data_path)
        else:
            print('Input offense categories could not be understood')  
        return
    
    def get_ruleset(self, ruleset : dict):
        self.ruleset = ruleset
        return
    
    def get_population_ids(self, data : str):
        return list(getattr(self, data)[self.id].unique())
    
    def process_time_vars(self, data_label : str, use_t_cols : list, merge : bool, clean_col_names : bool):
        # Get the df to process 
        df = getattr(self, data_label)
        
        # Clean all the column names
        if clean_col_names:
            df.columns = [utils.clean(col, remove = ['rape','\n']) for col in df.columns]
            self.id = utils.clean(self.id, remove = None)
        else:
            print('Since column names are not cleaned, several required variables for time calculations cannot be found')
            return 
        
        # Add id to the columns needed for calculation 
        use_t_cols.append(self.id)
        
        # Initialize calculations 
        calc_t_cols = []
        
        # Get the df to process 
        df = getattr(self, data_label)
        
        # Get the present date
        present_date = datetime.datetime.now()
        
        # Sentence duration in years
        for tc in use_t_cols:
            if "months" in tc.lower():
                df[tc.replace("months", "years")] = df[tc].apply(utils.month_to_year)
                calc_t_cols.append(tc.replace('months', 'years'))
                print(f"Calculation complete for: '{tc.replace('months', 'years')}'")
            
            if "birthday" in tc.lower():
                # Age of individual
                df['age in years'] = df[tc].apply(utils.years_between, y = present_date)
                calc_t_cols.append('age in years')
                print("Calculation complete for: 'age in years'")
            
            if "offense end date" in tc.lower():
                # Sentence served in years
                df['time served in years'] = df[tc].apply(utils.years_between, y = present_date)
                calc_t_cols.append('time served in years')
                print("Calculation complete for: 'time served in years'")
            
        # Try for the rest of the calculations since they require more than one column
        # Age at the time of offense
        if ("offense end date" in use_t_cols) and ("birthday" in use_t_cols):
            df['age during offense'] = df['offense end date'].apply(utils.years_between, y = df['birthday'])
            calc_t_cols.append('age during offense')
            print("Calculation complete for: 'age during offense'")
        
        # Expected release date
        if ("offense end date" in use_t_cols) and ("aggregate sentence in months" in use_t_cols):
            df['expected release date'] = utils.add_date_months_vec(df = df, date_col = 'offense end date', months_col = 'aggregate sentence in months')
            calc_t_cols.append('expected release date')
            print("Calculation complete for: 'expected release date'")
            
        # Return the resulting dataframe with the calculated time columns and the data with NaN/NaTs in these columns
        # If time variables are to be added to the entire input dataframe
        if merge: 
            setattr(self, data_label, df)
            return df, utils.incorrect_time(df = df, cols = calc_t_cols)
        # If time variables are to be stored in a separate dataframe
        else:
            setattr(self, data_label, df)
            return df[use_t_cols+calc_t_cols], utils.incorrect_time(df = df, cols = calc_t_cols)
    
    def apply_offense_rules(self, data, sel_off, how, prefix, offense_var, pop_ids):       
        # Get the appropriate raw dataset
        df = getattr(self, data)
        # Rule specific disqualifying IDs
        disqual_ids = []
        # Get the qualifying IDs thus far in the rule application process
        qual_ids = list(set(df[self.id].unique()).difference(set(self.disqual_ids)))
        df = df[df[self.id].isin(qual_ids)]

        # Remove prefix 
        df.loc[:, offense_var] = df[offense_var].str.replace(prefix, "")
        
        # Clean the column data 
        df.loc[:, offense_var] = utils.clean_blk(data = df[offense_var], remove = ['pc', 'rape', '\n', ' '])
        
        # Optimize for large datasets - use vectorized operations instead of groupby loop
        print(f"Processing {len(df)} records for offense rules")
        
        # Get the offense variable in the dataset that best matches the offense indicator 
        if how == "Exclude":
            # Vectorized approach: check if any offense in sel_off is present for each ID
            df_subset = df[[self.id, offense_var]].copy()
            df_subset['has_target_offense'] = df_subset[offense_var].isin(sel_off)
            # Group by ID and check if any offense matches
            id_has_offense = df_subset.groupby(self.id)['has_target_offense'].any()
            disqual_ids = id_has_offense[id_has_offense].index.tolist()
            
        elif how == "Include":
            # Vectorized approach: check if all offenses are in sel_off for each ID
            df_subset = df[[self.id, offense_var]].copy()
            df_subset['has_target_offense'] = df_subset[offense_var].isin(sel_off)
            # Group by ID and check if any offense does NOT match
            id_has_non_target = df_subset.groupby(self.id)['has_target_offense'].apply(lambda x: not x.all())
            disqual_ids = id_has_non_target[id_has_non_target].index.tolist()
            
        else: 
            print("Selection logic not understood")
        
        print(f"Identified {len(disqual_ids)} disqualifying IDs from {len(qual_ids)} IDs")
        # Add to the cohort's disqualifying IDs
        self.disqual_ids = list(set.union(set(self.disqual_ids), set(disqual_ids)))
        print(f"Number of resultant qualifying IDs from all rules applied thus far is {len(self.get_population_ids(pop_ids))} - {len(self.disqual_ids)} = {len(self.get_population_ids(pop_ids)) - len(self.disqual_ids)}")
        
        return self.disqual_ids
    
    def apply_sentence_length_rules(self, data, sentence_var, max_length, min_length, pop_ids):
        # Get the appropriate raw dataset
        df = getattr(self, data)
        # Get the qualifying IDs thus far in the rule application process
        qual_ids = list(set(df[self.id].unique()).difference(set(self.disqual_ids)))
        df = df[df[self.id].isin(qual_ids)]
        # Rule specific disqualifying IDs
        disqual_ids = []
        if not max_length: 
            max_length = df[sentence_var].max()
        if not min_length: 
            min_length = df[sentence_var].min()
        # Disqualifying IDs - opposite of criteria
        print(f"Finding IDs that are outside of the defined range: {max_length} to {min_length}")
        disqual_ids = df[(df[sentence_var] > max_length) | (df[sentence_var] < min_length)][self.id].unique()
        # Add to the cohort's disqualifying IDs
        print(f"Identified {len(disqual_ids)} disqualifying IDs from {len(qual_ids)} IDs")
        # Join the new disqualifying IDs with the existing disqualifying list
        self.disqual_ids = list(set.union(set(self.disqual_ids), set(disqual_ids)))
        print(f"Number of resultant qualifying IDs from all rules applied thus far is {len(self.get_population_ids(pop_ids))} - {len(self.disqual_ids)} = {len(self.get_population_ids(pop_ids)) - len(self.disqual_ids)}")

        return self.disqual_ids
        
    def apply_ruleset(self, prefix, clean_col_names, pop_ids, use_t_cols):
        # Initial empty list for disqualifying IDs that will be shared across all rules
        self.disqual_ids = []
        
        # Process time variables 
        self.process_time_vars(data_label = pop_ids, 
                                 use_t_cols = use_t_cols, 
                                 merge = True, 
                                 clean_col_names = True)
        
        # Process each criteria at a time
        for criteria_type in self.ruleset['criteria'].keys():
            print(f"Processing criteria type: {criteria_type}")
            # For offense related queries
            if ("commit" in criteria_type) or ("offense" in criteria_type):
                try:
                    # Get the variable with offenses data
                    offense_var = list(self.ruleset['criteria'][criteria_type].keys())[0]

                    # Get the raw dataset with offenses to query
                    data_label = self.ruleset['criteria'][criteria_type][offense_var]['data_label']+"_raw"
                    print(f"Extracting data from file labeled: {data_label}")
                    
                    # Get the selected offenses from the ruleset. "Type" and "Offenses" are columns in the selection criteria
                    sel_off = list(self.offense_categories[self.offense_categories["Type"].isin(self.ruleset['criteria'][criteria_type][offense_var]['types'])]["Offenses"])
                    
                    # Get inputs: permuations
                    try:
                        perm = self.ruleset['criteria'][criteria_type][offense_var]['implications']['perm']
                    except: 
                        perm = None 
                    # Get inputs: positions
                    try:
                        fix_pos = self.ruleset['criteria'][criteria_type][offense_var]['implications']['fix positions']
                    except: 
                        fix_pos = None
                    # Get inputs: permuations
                    try:
                        placeholder = self.ruleset['criteria'][criteria_type][offense_var]['implications']['placeholder']
                    except: 
                        placeholder = None

                    # Implied selections from permutations, etc. 
                    sel_off = impl.gen_impl_off(offenses = sel_off, 
                                                impl_rel = self.ruleset['criteria'][criteria_type][offense_var]['implications']['codes'],
                                                perm = perm, 
                                                fix_pos = fix_pos, 
                                                placeholder = placeholder,
                                                how = 'inclusive',
                                                sep = '',
                                                clean = True)
                    
                    # Get the logical query: Include, Exclude etc.
                    how = self.ruleset['criteria'][criteria_type][offense_var]['mode']
                    print(f"Selected column: {offense_var}; Raw dataset: {data_label}; Logic: {how}")
                    
                    # Clean col name from ruleset mapping
                    if clean_col_names:
                        offense_var = utils.clean(offense_var, remove = ['rape', '\n'])

                    # Apply the rules
                    _ = self.apply_offense_rules(data = data_label, 
                                                 how = how,
                                                 prefix = prefix,
                                                 sel_off = sel_off, 
                                                 offense_var = offense_var, 
                                                 pop_ids = pop_ids)     
                except Exception as e:
                    print(f"An error occurred: {e}")
                    
                    print("Full error description:\n")
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    trace = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    for line in trace:
                        print(line, end="")
                    pass  
            # For sentence length related queries
            elif "sentence" in criteria_type:
                try:
                    # Get the variable with sentence length data
                    sentence_var = list(self.ruleset['criteria'][criteria_type].keys())[0]
                    
                    # Get other values
                    data_label = self.ruleset['criteria'][criteria_type][sentence_var]['data_label']+"_raw"
                    print(f"Extracting data from file labeled: {data_label}")
                    max_length = self.ruleset['criteria'][criteria_type][sentence_var]['max']
                    min_length = self.ruleset['criteria'][criteria_type][sentence_var]['min']
                    print(f"Selected column: {sentence_var}; Raw dataset: {data_label}; Range: {min_length, max_length}")
                    
                    # Clean col name from ruleset mapping
                    if clean_col_names:
                        sentence_var = utils.clean(list(self.ruleset['criteria'][criteria_type].keys())[0], remove = ['rape', '\n'])
                    
                    # Apply the rules
                    _ = self.apply_sentence_length_rules(data = data_label,
                                                         sentence_var = sentence_var, 
                                                         max_length = max_length,
                                                         min_length = min_length, 
                                                         pop_ids = pop_ids)   
                except Exception as e:
                    print(f"An error occurred: {e}")
                    print("Full error description:\n")
                    exc_type, exc_value, exc_traceback = sys.exc_info()
                    trace = traceback.format_exception(exc_type, exc_value, exc_traceback)
                    for line in trace:
                        print(line, end="")
                    pass      
            else: 
                print("Cannot process criteria type")
            
            print(f"Processing complete for criteria {criteria_type}\n")
        return
    
    def get_responsive_data(self, input_data_path : dict):
        for cat in input_data_path.keys():
            print(f"Retrieving qualifying records for: {cat}")
            raw_df = getattr(self, cat+"_raw")
            # Find qualifying records
            resp_df = raw_df[~raw_df[self.id].isin(self.disqual_ids)]
            # Set the data tables and assign them to the respective categories
            setattr(self, cat, resp_df)
        return
    
    def write_responsive_data(self, input_data_path, output_data_path : dict, file_format : str):
        for file_name in output_data_path.keys():
            if file_format == 'xlsx':
                for cat in input_data_path.keys():
                    pd.to_excel(output_data_path[file_name])
                    print(f"Wrote output for {cat} to {file_name}{file_format}")
            elif file_format == ".csv":
                for cat in input_data_path.keys():
                    pd.to_csv(output_data_path[file_name])
                    print(f"Wrote output for {cat} to {file_name}{file_format}")
    
    def generate_ruleset_summary(self):
        summary_parts = []
        criteria = self.ruleset["criteria"]
        
        # Prior commitments
        if criteria["prior_commitments"]["Offense"]["types"]:
            prior_mode = "not in" if criteria["prior_commitments"]["Offense"]["mode"] == "Exclude" else "in"
            prior_types = ", ".join(criteria["prior_commitments"]["Offense"]["types"])
            summary_parts.append(f"Prior offenses {prior_mode} {prior_types}")
        
        # Current commitments
        if criteria["current_commitments"]["Offense"]["types"]:
            current_mode = "not in" if criteria["current_commitments"]["Offense"]["mode"] == "Exclude" else "in"
            current_types = ", ".join(criteria["current_commitments"]["Offense"]["types"])
            summary_parts.append(f"Current offenses {current_mode} {current_types}")
        
        # Controlling offense
        if criteria["controlling_offense"]["Controlling Offense"]["types"]:
            ctrl_mode = "not in" if criteria["controlling_offense"]["Controlling Offense"]["mode"] == "Exclude" else "in"
            ctrl_types = ", ".join(criteria["controlling_offense"]["Controlling Offense"]["types"])
            summary_parts.append(f"Controlling offenses {ctrl_mode} {ctrl_types}")
        
        # Sentence length
        min_len = criteria["sentence_length"]["Aggregate Sentence in Months"]["min"]
        max_len = criteria["sentence_length"]["Aggregate Sentence in Months"]["max"]
        if min_len > 240 or max_len < 10000000:
            summary_parts.append(f"Sentence length between {min_len} and {max_len} months")
        
        # Sentence served
        min_served = criteria["sentence_served"]["time served in years"]["min"]
        max_served = criteria["sentence_served"]["time served in years"]["max"]
        if min_served > 10 or max_served < 10000000:
            summary_parts.append(f"Time served between {min_served} and {max_served} years")
        
        self.ruleset_summary = summary_parts
        return