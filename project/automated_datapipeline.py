import pandas as pd
import numpy as np
import requests
import zipfile
import os
from sqlalchemy import create_engine

def download_and_extract(url, extract_to):
    """
    Download a ZIP files from a given URL and extract its content to a specified directory.
    
    Args:
        url (str): URL of the ZIP file to download.
        extract_to (str): Directory where the contents of the ZIP file will be extracted.

    Returns:
        None
    """
    local_zip_path = os.path.join(extract_to, os.path.basename(url))
    response = requests.get(url)
    with open(local_zip_path, 'wb') as file:
        file.write(response.content)
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    os.remove(local_zip_path)

def load_and_clean_data(file_path, delimiter=','):
    """
    Load a CSV file into a pandas DataFrame and drop rows with missing values.
    
    Args:
        file_path (str): Path to the CSV file to read the data.
        delimiter (str): Delimiter used in the CSV file. Default is ','.
        
    Returns:
        pd.DataFrame: Cleaned DataFrame with no missing values.
    """
    df = pd.read_csv(file_path, delimiter=delimiter)
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
    df = df[(df[columns] != 0).all(axis=1)]
    return df

def fetch_data_from_db(db_path, table_name):
    """
    Fetch data from a specified table in a SQLite database and load it into a pandas DataFrame.
    
    Args:
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the table in the SQL database where the data will be written.
        
    Returns:
        pd.DataFrame: DataFrame containing the data from the specified table.
    """ 
    engine = create_engine(f'sqlite:///{db_path}')
    df = pd.read_sql_table(table_name, engine)
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
    # Add a new column for quarters
    df['Quarter'] = df['Month'].map(month_to_quarter)
    return df

def transform_wildfire_data(file_path):
    """
    Transforms Wildfire burned area data in a pandas DataFrame 

    This function performs several operations on the DataFrame to prepare this data for analysis:
    1. Drop the uncessary columns.
    2. Drops the zero values from the rows with specified columns.
    3. Renames columns for consistency and clarity
    4. Creating quaterly column.
    5. Reorder the columns. 
    
    Args:
        file_path (str): Path to the CSV file containing wildfire data.

    Returns:
        pd.DataFrame: Transformed wildfire DataFrame.
    """
    wildfire_df = load_and_clean_data(file_path)
    wildfire_columns_to_drop = ['gid_1']
    wildfire_df = drop_irrelevant_columns(wildfire_df, wildfire_columns_to_drop)
    columns_with_zeros = ["forest", "savannas", "shrublands_grasslands", "croplands", "other"]
    wildfire_df = drop_rows_with_zeros(wildfire_df, columns_with_zeros)
    wildfire_columns_rename = {'year': 'Year',
                                    'month':'Month',
                                    'gid_0':'Country_Code',
                                    'country':'Country_Name',
                                    'region':'Region_Name',
                                    'forest':'Forest_Burned_Area',
                                    'savannas':'Savannas_Burned_Area',
                                    'shrublands_grasslands':'Shrublands_Grasslands_Burned_Area',
                                    'croplands':'Croplands_Burned_Area',
                                    'other':'Other_Burned_Area'}
    wildfire_df = rename_columns(wildfire_df, wildfire_columns_rename)
    wildfire_df = create_quarterly_column(wildfire_df)
    wildfire_df = wildfire_df.reindex(columns=['Year','Month','Quarter','Country_Code','Country_Name',
                                               'Region_Name','Forest_Burned_Area','Savannas_Burned_Area', 
                                               'Shrublands_Grasslands_Burned_Area', 'Croplands_Burned_Area', 
                                               'Other_Burned_Area'])
    return wildfire_df

def transform_emissions_data(file_path):
    """
   Transforms Emissions data in a pandas DataFrame 

    This function performs several operations on the DataFrame to prepare this data for analysis:
    1. Drop the uncessary columns.
    2. Drops the zero values from the rows with specified columns.
    3. Renames columns for consistency and clarity
    4. Creating quaterly column.
    5. Reorder the columns. 
    
    Args:
        file_path (str): Path to the CSV file containing emissions data.

    Returns:
        pd.DataFrame: Transformed emissions DataFrame.
    """
    emissions_df = load_and_clean_data(file_path)
    emissions_columns_to_drop = ['gid_1']
    emissions_df = drop_irrelevant_columns(emissions_df, emissions_columns_to_drop)
    columns_with_zeros = ["CO2", "CO", "TPM", "PM25", "TPC", "NMHC", "OC", "CH4", "SO2", "BC", "NOx"]
    emissions_df = drop_rows_with_zeros(emissions_df, columns_with_zeros)
    emissions_columns_rename = {'year':'Year',
                                'month':'Month',
                                'gid_0':'Country_Code' ,
                                'country':'Country_Name',
                                'region':'Region_Name'}
    emissions_df = rename_columns(emissions_df, emissions_columns_rename)
    emissions_df = create_quarterly_column(emissions_df)
    emissions_df = emissions_df.reindex(columns=['Year', 'Month', 'Quarter', 'Country_Code', 'Country_Name', 
                                                 'Region_Name', 'CO2', 'CO', 'TPM', 'PM25', 'TPC', 'NMHC', 
                                                 'OC', 'CH4', 'SO2', 'BC', 'NOx'])
    return emissions_df

def main():
    """
    Main function to execute the data engineering automated pipeline. This function downloads,
    extracts, transforms, and saves wildfire and emissions data.
    
    Steps:
        - Download and extract data.
        - Transform wildfire and emissions data.
        - Save the transformed data to a SQLite database.
        - Fetch data from the database to verify and save to CSV files.
    
    Returns:
        None
    """
    wildfire_url = "https://effis-gwis-cms.s3.eu-west-1.amazonaws.com/apps/country.profile/MCD64A1_burned_area_full_dataset_2002-2023.zip"
    emissions_url = "https://effis-gwis-cms.s3.eu-west-1.amazonaws.com/apps/country.profile/emission_gfed_full_2002_2023.zip"
   
    #Directory for downloading and extracting data
    download_path = "data"
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    download_and_extract(wildfire_url, download_path)
    download_and_extract(emissions_url, download_path)
    
    #File paths for the extracted CSV files
    wildfire_file = os.path.join(download_path, 'MCD64A1_burned_area_full_dataset_2002-2023.csv')
    emissions_file = os.path.join(download_path, 'emission_gfed_full_2002_2023.csv')
   
    #Transform the wildfire and emissions data
    wildfire_df = transform_wildfire_data(wildfire_file)
    emissions_df = transform_emissions_data(emissions_file)

    # Merge the wildfire and emission datasets to perform further analysis
    merged_df = pd.merge(wildfire_df, emissions_df, how='inner')  
    
    #SQLite database path
    db_path = os.path.join(download_path, 'wildfire_emissions_data.db')
    engine = create_engine(f'sqlite:///{db_path}')

    #Save the processed data to the database
    wildfire_df.to_sql('wildfire_data', engine, index=False, if_exists='replace')
    emissions_df.to_sql('emissions_data', engine, index=False, if_exists='replace')
    merged_df.to_sql('merged_table',engine,index=False, if_exists='replace')
    
    #Fetch data from the database to verify and save to CSV
    fetched_wildfire_df = fetch_data_from_db(db_path, 'wildfire_data')
    fetched_emissions_df = fetch_data_from_db(db_path, 'emissions_data')
    fetched_merged_df = fetch_data_from_db(db_path, 'merged_table')
    
    fetched_wildfire_df.to_csv(os.path.join(download_path, 'wildfire_burned_area_data.csv'), index=False)
    fetched_emissions_df.to_csv(os.path.join(download_path, 'emissions_data.csv'), index=False)
    fetched_merged_df.to_csv(os.path.join(download_path, 'merged_data.csv'), index=False)

if __name__ == "__main__":
    main()
