import pandas as pd
import requests
import zipfile
import os
from sqlalchemy import create_engine

# Function to download and extract a ZIP file from a URL
def download_and_extract(url, extract_to):
    local_zip_path = os.path.join(extract_to, os.path.basename(url))
    response = requests.get(url)
    with open(local_zip_path, 'wb') as file:
        file.write(response.content)
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    os.remove(local_zip_path)

#Function to load CSV file into a DataFrame and drop rows with missing values
def load_and_clean_data(file_path, delimiter=','):
    df = pd.read_csv(file_path, delimiter=delimiter)
    df.dropna(inplace=True)
    return df

#Function to drop specified columns
def drop_irrelevant_columns(df, columns_to_drop):
    df.drop(columns=columns_to_drop, inplace=True)
    return df

#Function to rename columns
def rename_columns(df, columns_mapping):
    df.rename(columns=columns_mapping, inplace=True)
    return df

#function to drop rows with zero values
def drop_rows_with_zeros(df, columns):
    df = df[(df[columns] != 0).all(axis=1)]
    return df

#Function to fetch data from a SQLite database table  into a Dataframe
def fetch_data_from_db(db_path, table_name):
    engine = create_engine(f'sqlite:///{db_path}')
    df = pd.read_sql_table(table_name, engine)
    return df

#Function to Transform burned area data
def transform_burned_area_data(file_path):
    burned_area_df = load_and_clean_data(file_path)
    burned_area_columns_to_drop = ['gid_1']
    burned_area_df = drop_irrelevant_columns(burned_area_df, burned_area_columns_to_drop)
    columns_with_zeros = ["forest", "savannas", "shrublands_grasslands", "croplands", "other"]
    burned_area_df = drop_rows_with_zeros(burned_area_df, columns_with_zeros)
    burned_area_columns_rename = {'year': 'Year',
                                    'month':'Month',
                                    'gid_0':'Country_Code',
                                    'country':'Country_Name',
                                    'region':'Region_Name',
                                    'forest':'Forest_Burned_Area',
                                    'savannas':'Savannas_Burned_Area',
                                    'shrublands_grasslands':'Shrublands_Grasslands_Burned_Area',
                                    'croplands':'Croplands_Burned_Area',
                                    'other':'Other_Burned_Area'}
    burned_area_df = rename_columns(burned_area_df, burned_area_columns_rename)
    return burned_area_df

#Function to transform emissions data
def transform_emissions_data(file_path):
    emissions_df = load_and_clean_data(file_path)
    emissions_columns_to_drop = ['gid_1']
    emissions_df = drop_irrelevant_columns(emissions_df, emissions_columns_to_drop)
    columns_with_zeros = ["CO2", "CO", "TPM", "PM25", "TPC", "NMHC", "OC", "CH4", "SO2", "BC", "NOx"]
    emissions_df = drop_rows_with_zeros(emissions_df, columns_with_zeros)
    emissions_columns_rename = {'year':'Year',
                                'month':'Month',
                                'gid_0':'Country_Code' ,
                                'country':'Country_Name',
                                'region':'Region'}
    emissions_df = rename_columns(emissions_df, emissions_columns_rename)
    return emissions_df


def main():
    burned_area_url = "https://effis-gwis-cms.s3.eu-west-1.amazonaws.com/apps/country.profile/MCD64A1_burned_area_full_dataset_2002-2023.zip"
    emissions_url = "https://effis-gwis-cms.s3.eu-west-1.amazonaws.com/apps/country.profile/emission_gfed_full_2002_2023.zip"
   
    #Directory for downloading and extracting data
    download_path = "data"
    if not os.path.exists(download_path):
        os.makedirs(download_path)

    download_and_extract(burned_area_url, download_path)
    download_and_extract(emissions_url, download_path)
    
    #File paths for the extracted CSV files
    burned_area_file = os.path.join(download_path, 'MCD64A1_burned_area_full_dataset_2002-2023.csv')
    emissions_file = os.path.join(download_path, 'emission_gfed_full_2002_2023.csv')
   
    #Transform the emissions and burned area data
    burned_area_df = transform_burned_area_data(burned_area_file)
    emissions_df = transform_emissions_data(emissions_file)
    
    #SQLite database path
    db_path = os.path.join(download_path, 'cleaned_data.db')
    engine = create_engine(f'sqlite:///{db_path}')

    #Save the processed data to the database
    burned_area_df.to_sql('burned_area_data', engine, index=False, if_exists='replace')
    emissions_df.to_sql('emissions_data', engine, index=False, if_exists='replace')
    
    #Fetch data from the database to verify and save to CSV
    fetched_burned_area_df = fetch_data_from_db(db_path, 'burned_area_data')
    fetched_emissions_df = fetch_data_from_db(db_path, 'emissions_data')
    
    fetched_burned_area_df.to_csv(os.path.join(download_path, 'Wildfire_Burned_Area_Data.csv'), index=False)
    fetched_emissions_df.to_csv(os.path.join(download_path, 'Wildfire_Emissions_Data.csv'), index=False)
    
if __name__ == "__main__":
    main()