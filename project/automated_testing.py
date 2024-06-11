import os
import sqlite3
import unittest
import pandas as pd
from automated_datapipeline import main, download_and_extract_in_memory, load_and_clean_data, transform_wildfire_data, transform_emissions_data


def check_num_cols(df, num):
    """
    Checks if the number of columns in a DataFrame matches the expected number of columns.

    Args:
        df (pandas.DataFrame): Used to check the DataFrame.
        num (int): Specifies expected number of columns.

    Raises:
        AssertionError: If the number of columns doesn't match the expected number.
    """
    assert df.shape[1] == num, f"Number of columns should be {num}" 


def check_column_names(df, expected_column_names):
    """
    Checks if the column names in a DataFrame matches the expected column names.

    Args:
        df (pandas.DataFrame): Used to check the DataFrame.
        expected_column_names (list of str): The expected column names list in the DataFrame.

    Raises:
        AssertionError: If the column names don't match the expected names.
    """
    for x, y in zip(expected_column_names, df.columns):
        assert x == y, f"Column name incorrect: {y} instead of {x}"


def check_null_values(df, cols):
    """
    Checks if any of the specified columns in a DataFrame contains a null values.

    Args:
        df (pandas.DataFrame): Used to check the DataFrame.
        cols (list of str): The names of the columns to check for null values.

    Raises:
        AssertionError: If any of the specified columns contain null values.
    """
    for col in cols:
        assert not df[col].isna().any(), f"Column {col} contains null values"
    
        
def check_month_values(df):
    """
    Checks if the values in the 'Month' column of a DataFrame are within the range 1 to 12.

    Args:
        df (pandas.DataFrame): Used to check the DataFrame.

    Raises:
        AssertionError: If any value in the 'Month' column is outside the range 1 to 12.
    """
    assert df['Month'].between(1, 12).all(), "Month column contains values outside 1 to 12"


def read_sql_table(db_path, table_name):
    """
    Reads a table from an SQLite database into a pandas DataFrame.

    Args:
        db_path (str): The path to the SQLite database file.
        table_name (str): The name of the table to read.

    Returns:
        pandas.DataFrame: The DataFrame containing the table data.
    """
    query = f"SELECT * FROM {table_name}"
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def check_wildfire_data_table(wildfire_df):
    """
    Checks the structure and integrity of a DataFrame containing wildfire data.

    Args:
        wildfire_df (pandas.DataFrame): The DataFrame containing wildfire data.

    Raises:
        AssertionError: If the DataFrame fails any of the integrity checks.
    """
    check_num_cols(wildfire_df, 11)
    wildfire_expected_columns = ['Year', 'Month', 'Quarter', 'Country_Code', 'Country_Name', 'Region_Name', 'Forest_BA', 'Savannas_BA', 'Shrubs_Grasslands_BA', 'Croplands_BA', 'Other_BA']
    check_column_names(wildfire_df, wildfire_expected_columns)
    check_null_values(wildfire_df, wildfire_df.columns)
    check_month_values(wildfire_df)


def check_emissions_data_table(emissions_df):
    """
    Checks the structure and integrity of a DataFrame containing emissions data.

    Args:
        emissions_df (pandas.DataFrame): The DataFrame containing emissions data.

    Raises:
        AssertionError: If the DataFrame fails any of the integrity checks.
    """
    check_num_cols(emissions_df, 17)
    emissions_expected_columns = ['Year', 'Month', 'Quarter', 'Country_Code', 'Country_Name', 'Region_Name', 'CO2', 'CO', 'TPM', 'PM25', 'TPC', 'NMHC', 'OC', 'CH4', 'SO2', 'BC', 'NOx']
    check_column_names(emissions_df, emissions_expected_columns)
    check_null_values(emissions_df, emissions_df.columns)
    check_month_values(emissions_df)


class TestDataPipeline(unittest.TestCase):
    """
    A test case class for testing the data pipeline.

    This class contains test methods to validate the execution of the data pipeline
    and the integrity of the output data files and tables.
    """
    @classmethod
    def setUpClass(cls):
        """
        Set up the test environment by executing the data pipeline.

        This method is called once before any tests in the class are run.
        """
        main()


    def test_output_files_exist(self):
        """
        Test whether the expected output files and tables exist.

        This test method checks if the database file and the required tables exist
        after executing the data pipeline.
        """
        # Check if the database file exists
        db_file = 'data/wildfire_burnedarea_emissions_data.db'
        self.assertTrue(os.path.isfile(db_file), f"Database file '{db_file}' does not exist")

        # Check if the tables exist within the database
        with sqlite3.connect(db_file) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            tables = [table[0] for table in tables]

        expected_tables = ['wildfire_burnedarea_data', 'wildfire_emissions_data', 'wildfire_merged_data']
        for table in expected_tables:
            self.assertIn(table, tables, f"Table '{table}' does not exist in the database")
        print("Test output_files_exist passed successfully.")


    def test_pipeline_execution(self):
        """
        Test the execution of the data pipeline.

        This test method executes the data pipeline, including data loading,
        transformation, and validation, and ensures that the output data tables
        meet the expected structure and integrity criteria.
        """
        wildfire_url = "https://effis-gwis-cms.s3.eu-west-1.amazonaws.com/apps/country.profile/MCD64A1_burned_area_full_dataset_2002-2023.zip"
        emissions_url = "https://effis-gwis-cms.s3.eu-west-1.amazonaws.com/apps/country.profile/emission_gfed_full_2002_2023.zip"

        wildfire_files = download_and_extract_in_memory(wildfire_url)
        emissions_files = download_and_extract_in_memory(emissions_url)

        wildfire_file_like = wildfire_files['MCD64A1_burned_area_full_dataset_2002-2023.csv']
        emissions_file_like = emissions_files['emission_gfed_full_2002_2023.csv']

        wildfire_df = load_and_clean_data(wildfire_file_like)
        emissions_df = load_and_clean_data(emissions_file_like)

        wildfire_df = transform_wildfire_data(wildfire_df)
        emissions_df = transform_emissions_data(emissions_df)
        
        # Check wildfire data table
        check_wildfire_data_table(wildfire_df)

        # Check emissions data table
        check_emissions_data_table(emissions_df)
    

        print("Test pipeline_execution passed successfully.")


if __name__ == '__main__':
    unittest.main()
