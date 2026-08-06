# -*- coding: utf-8 -*-
import pandas as pd

def load_data(url):
    """
    Parameters
    ----------
    url : str
        URL or file path pointing to a CSV or Excel (.xlsx) file
    
    Returns
    -------
    df : pd.DataFrame or None
        DataFrame loaded from the specified file. Returns None and prints a message
        if the file type is neither CSV nor Excel
    """    
    if '.csv' in url:
        return pd.read_csv(url)
    elif '.xlsx' in url:
        return pd.read_excel(url)
    else: 
        print("URL is neither CSV nor Excel file and cannot be read")
        return
        
def clean(data, remove = ['pc', 'rape', '\n', ' ']):
    """

    Parameters
    ----------
    data : str
        A single string. Example: An offense value 'PC123 (A).(1).'
    remove : list, optional
        List of values to be removed from the input string. Default is ['pc', 'rape', '\n', ' ']
        
    Returns
    -------
    data : str
        Lower-case string without trailing periods and the contents specified in remove parameter
        For example, the input string 'PC123 (A).(1).' will return 'pc123(a).(1)'

    """
    # Lowercase all letters
    data = str(data).lower()
    # Remove trailing periods
    data = data.rstrip('.')
    # Remove info specified 
    if remove:
        for r in remove: 
            if r in data:
                data = data.replace(r, '')
    
    return data


def clean_blk(data, 
              inplace = False, 
              names = None, 
              remove = ['pc', 'rape', '\n', ' ']):
    """

    Parameters
    ----------
    data : str, list, pandas dataframe or pandas series
        Bulk data with string contents to be cleaned
    names : dict, optional
        Only applicable when input data is a pandas dataframe
        Contains key:value pairs wherein keys correspond to the names of columns in the input dataframe that should be cleaned and values correspond to the new column names
        Default is None. All columns will be cleaned and the suffix ' cleaned' will be attached to the new column names
    inplace : boolean, optional
        Only applicable when input data is a pandas dataframe. Specify whether to return a new and separate dataframe or modify the existing one
        Default is False
        
    Returns
    -------
    data : str, list, pandas dataframe or pandas series (corresponding to input data)
        Applies the clean() function on each string in the input and returns the modified values with the same input type, i.e. if a pandas series is passed the result will be a pandas series with modified strings

    """
    # If input is a single string
    if isinstance(data, str):
        return clean(data = data, remove = remove)
    
    # If input is a list of strings
    elif isinstance(data, list):
        return [clean(data=off, remove=remove) for off in data]
    
    # If input is a column of a pandas dataframe
    elif isinstance(data, pd.Series):
        # Match clean() exactly, including its conversion of missing values to
        # strings, while using pandas string operations instead of Series.apply.
        data_clean = data.astype(str).str.lower().str.rstrip(".")
        for value in remove or []:
            data_clean = data_clean.str.replace(value, "", regex=False)
        return data_clean
    
    # If input is a pandas dataframe
    elif isinstance(data, pd.DataFrame):
        # If names is not passed, function will default to cleaning all columns and adding the suffix ' cleaned' to the new columns
        if not names:
            names = {}
            for col in data.columns:
                names[col] = col+ ' cleaned'
        # Modify the existing dataframe, i.e. add new columns
        if inplace:
            # Apply the cleaning function onto each column specified
            for col in names.keys():
                data[names[col]] = clean_blk(data[col], remove=remove)
            return data
        # Create a separate dataframe with the modified columns and leave the existing one unchanged
        else:
            data_new = data.copy()
            # Apply the cleaning function onto each column specified
            for col in names.keys():
                data_new[names[col]] = clean_blk(data[col], remove=remove)
            return data_new
        

def clean_var_names(val, rem = None):
    """
    Parameters
    ----------
    val : str or list
        Input string or list of strings to be cleaned
    rem : list, optional
        List of additional substrings to remove from the value(s) after standard cleaning.
        Default is None
    
    Returns
    -------
    mod_val : str or list
        Cleaned string or list of strings with underscores, hyphens, and forward slashes
        replaced by spaces, trailing periods removed, and all characters lowercased.
        If rem is provided, the specified substrings are also removed
    """
    if type(val) is list:
        mod_val = []
        for v in val: 
            try:
                v = v.replace("_", " ").replace("-", " ").replace("/", " ").rstrip(".").lower()
                if rem: 
                    for r in rem: 
                        v = v.replace(r, "")
            except:
                pass
            mod_val.append(v)
        return mod_val
    elif type(val) is str:
        v = val
        try:
            v = v.replace("_", " ").replace("-", " ").replace("/", " ").rstrip(".").lower()
            if rem: 
                for r in rem: 
                    v = v.replace(r, "")
        except:
            pass
        return v
