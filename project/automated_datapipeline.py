import pandas as pd
import numpy as np
import requests
import zipfile
import io
from sqlalchemy import create_engine
import os

def download_and_extract_in_memory(url):
    """
    Downloads a ZIP file from the specified URL and extracts its contents into memory.

    Parameters:
    url (str): The URL of the ZIP file to be downloaded.

    Returns:
    dict: A dictionary where the keys are the names of the files within the ZIP archive, 
          and the values are in-memory file-like objects containing the file contents."""

    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        return {name: io.BytesIO(zip_ref.read(name)) for name in zip_ref.namelist()}

def load_and_clean_data(file_like, delimiter=','):
    """
    Load data from a CSV file-like object into a pandas DataFrame and perform data cleaning.

    Parameters:
    file_like (file-like object): The file-like object containing the CSV data.
    delimiter (str, optional): The delimiter used in the CSV file. Default is ','.

    Returns:
    pd.DataFrame: A cleaned DataFrame with no missing values.
    """
    df = pd.read_csv(file_like, delimiter=delimiter)
    df.dropna(inplace=True)
    return df

def drop_irrelevant_columns(df, columns_to_drop):
    """
    Removes specified columns from a pandas DataFrame.
    
    Args:
        df (pd.DataFrame): The Dataframe from which coulumns are to be removed.
        columns_to_drop (list): list of strings represents the names of the columns to be removed.
    
    Returns:
        pd.DataFrame: Modified DataFrame with specified columns dropped.
    """
    df.drop(columns=columns_to_drop, inplace=True)
    return df

def rename_columns(df, columns_mapping):
    """
    Rename columns in a DataFrame according to a provided mapping.
    
    Args:
        df (pd.DataFrame): The Dataframe whose columns are to be renamed
        columns_mapping (dict): Dictionary mapping old column names to new column names.
        
    Returns:
        pd.DataFrame: DataFrame with columns renamed.
    """
    df.rename(columns=columns_mapping, inplace=True)
    return df

def drop_rows_with_zeros(df, columns):
    """
    Drop rows from a DataFrame that contain zero values in specified columns.
    
    Args:
        df (pd.DataFrame): Input DataFrame.
        columns (list): List of columns to check for zero values.
        
    Returns:
        pd.DataFrame: DataFrame with rows containing zero values in specified columns dropped.
    """
    df = df.loc[~(df[columns] == 0).all(axis=1)]
    return df

def create_quarterly_column(df):
    """
    Creates a new column indicating the corresponding quarter for each month in a DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame containing a 'Month' column.
        
    Returns:
        pd.DataFrame: DataFrame with a new 'Quarter' column indicating the corresponding quarter.
    """
    month_to_quarter = {
        1: 'Q1', 2: 'Q1', 3: 'Q1',
        4: 'Q2', 5: 'Q2', 6: 'Q2',
        7: 'Q3', 8: 'Q3', 9: 'Q3',
        10: 'Q4', 11: 'Q4', 12: 'Q4'
    }
    df['Quarter'] = df['Month'].map(month_to_quarter)
    return df

def create_database(db_dir, db_name):
    """
    Create a SQLite database in the specified directory.

    Parameters:
    db_dir (str): Directory path where the database will be created.
    db_name (str): Name of the SQLite database.

    Returns:
    str: Path of the created SQLite database.
    """
    db_path = os.path.join(db_dir, f"{db_name}.db")
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    return db_path

def transform_wildfire_data(df):
    """
    Transforms Wildfire burned area data in a pandas DataFrame 

    This function performs several operations on the DataFrame to prepare this data for analysis:
    1. Drop the uncessary columns.
    2. Drops the zero values from the rows with specified columns.
    3. Renames columns for consistency and clarity
    4. Creating quaterly column.
    5. Reorder the columns. 
    
    Args:
        df (str): Path to the CSV file containing wildfire data.

    Returns:
        pd.DataFrame: Transformed wildfire DataFrame.
    """
    wildfire_columns_to_drop = ['gid_1']
    df = drop_irrelevant_columns(df, wildfire_columns_to_drop).copy()
    columns_with_zeros = ["forest", "savannas", "shrublands_grasslands", "croplands", "other"]
    df = drop_rows_with_zeros(df, columns_with_zeros).copy()
    wildfire_columns_rename = {'year': 'Year',
                                    'month':'Month',
                                    'gid_0':'Country_Code',
                                    'country':'Country_Name',
                                    'region':'Region_Name',
                                    'forest':'Forest_BA',
                                    'savannas':'Savannas_BA',
                                    'shrublands_grasslands':'Shrubs_Grasslands_BA',
                                    'croplands':'Croplands_BA',
                                    'other':'Other_BA'}
    df = rename_columns(df, wildfire_columns_rename).copy()
    df = create_quarterly_column(df).copy()
    return df.reindex(columns=['Year','Month','Quarter','Country_Code','Country_Name',
                                               'Region_Name','Forest_BA','Savannas_BA', 
                                               'Shrubs_Grasslands_BA', 'Croplands_BA', 
                                               'Other_BA'])
    
def transform_emissions_data(df):
    """
    Transforms Emissions data in a pandas DataFrame 

    This function performs several operations on the DataFrame to prepare this data for analysis:
    1. Drop the uncessary columns.
    2. Drops the zero values from the rows with specified columns.
    3. Renames columns for consistency and clarity
    4. Creating quaterly column.
    5. Reorder the columns. 
    
    Args:
        df (str): Path to the CSV file containing emissions data.

    Returns:
        pd.DataFrame: Transformed emissions DataFrame.
    """
    emissions_columns_to_drop = ['gid_1']
    df = drop_irrelevant_columns(df, emissions_columns_to_drop).copy()
    columns_with_zeros = ["CO2", "CO", "TPM", "PM25", "TPC", "NMHC", "OC", "CH4", "SO2", "BC", "NOx"]
    df = drop_rows_with_zeros(df, columns_with_zeros).copy()
    emissions_columns_rename = {'year':'Year',
                                'month':'Month',
                                'gid_0':'Country_Code' ,
                                'country':'Country_Name',
                                'region':'Region_Name'}
    df = rename_columns(df, emissions_columns_rename).copy()
    df = create_quarterly_column(df).copy()
    return df.reindex(columns=['Year', 'Month', 'Quarter', 'Country_Code', 'Country_Name', 
                                                 'Region_Name', 'CO2', 'CO', 'TPM', 'PM25', 'TPC', 'NMHC', 
                                                 'OC', 'CH4', 'SO2', 'BC', 'NOx'])

def main():
    """
    Main function to execute the data engineering automated pipeline. This function downloads,
    extracts, transforms, and saves wildfire and emissions data.
    
    Steps:
        - Download and extract data.
        - Transform wildfire and emissions data.
        - Save the transformed data to a SQLite database.
    
    Returns:
        None
    """
    wildfire_url = "https://effis-gwis-cms.s3.eu-west-1.amazonaws.com/apps/country.profile/MCD64A1_burned_area_full_dataset_2002-2023.zip"
    emissions_url = "https://effis-gwis-cms.s3.eu-west-1.amazonaws.com/apps/country.profile/emission_gfed_full_2002_2023.zip"
    
    wildfire_files = download_and_extract_in_memory(wildfire_url)
    emissions_files = download_and_extract_in_memory(emissions_url)
    
    #File paths for the extracted CSV files
    wildfire_file_like = wildfire_files['MCD64A1_burned_area_full_dataset_2002-2023.csv']
    emissions_file_like = emissions_files['emission_gfed_full_2002_2023.csv']
    
    wildfire_df = load_and_clean_data(wildfire_file_like)
    emissions_df = load_and_clean_data(emissions_file_like)
    
    #Transform the wildfire and emissions data
    wildfire_df = transform_wildfire_data(wildfire_df).copy()
    emissions_df = transform_emissions_data(emissions_df).copy()
    
    # Merge the wildfire and emission datasets to perform further analysis
    merged_df = pd.merge(wildfire_df, emissions_df, how='inner')

    #SQLite database path
    db_dir = 'data'
    db_name = 'wildfire_burnedarea_emissions_data'
    db_path = create_database(db_dir, db_name)
    engine = create_engine(f'sqlite:///{db_path}')
    
    #Save the processed data to the database
    wildfire_df.to_sql('wildfire_burnedarea_data', engine, index=False, if_exists='replace')
    emissions_df.to_sql('wildfire_emissions_data', engine, index=False, if_exists='replace')
    merged_df.to_sql('wildfire_merged_data', engine, index=False, if_exists='replace')

if __name__ == "__main__":
    main()
