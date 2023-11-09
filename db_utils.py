import yaml
from sqlalchemy import create_engine, text
import pandas as pd
from typing import Dict, Any

class RDSDatabaseConnector:
    """
    A class for connecting to an AWS RDS database and performing operations.

    Parameters
    ----------
    - credentials_file (str): The path to the YAML file containing database credentials.
    - credentials (Dict[str, Any]): Database credentials loaded from the YAML file.
    - engine (sqlalchemy.engine.Engine): SQLAlchemy database engine for database operations.
    """

    def __init__(self, credentials_file: str = 'credentials.yaml') -> None:
        """
        Initialize the RDSDatabaseConnector.

        Parameters
        ----------
        - credentials_file (str): The path to the YAML file containing database credentials.
        """
        self.credentials = self._read_database_credentials(credentials_file)
        self.engine = self._initialize_database_engine()

    def _read_database_credentials(self, yaml_path: str) -> Dict[str, Any]:
        """
        Read database credentials from a YAML file.

        Parameters
        ----------

        - yaml_path (str): The path to the YAML file containing database credentials.

        Returns
        -------

        - Dict[str, Any]: Database credentials as a dictionary.
        """
        with open(yaml_path, 'r') as file:
            credentials = yaml.safe_load(file)
        return credentials

    def _initialize_database_engine(self) -> create_engine:
        """
        Initialize the SQLAlchemy database engine.

        Returns
        -------
        - create_engine: SQLAlchemy database engine for database operations.
        """
        db_url = (
            f"postgresql://{self.credentials['RDS_USER']}:{self.credentials['RDS_PASSWORD']}"
            f"@{self.credentials['RDS_HOST']}:{self.credentials['RDS_PORT']}/{self.credentials['RDS_DATABASE']}"
        )
        engine = create_engine(db_url)
        return engine

    def extract_data_to_dataframe(self, query: str) -> pd.DataFrame:
        """
        Extract data from the RDS database and return it as a Pandas DataFrame.

        Parameters
        ----------
        - query (str): SQL query for data extraction.

        Returns
        -------
        - pd.DataFrame: Extracted data as a Pandas DataFrame.
        """
        with self.engine.connect() as conn:
            data_frame = pd.read_sql_query(text(query), conn)
        return data_frame

    def save_to_csv(self, data_frame: pd.DataFrame, file_path: str) -> None:
        """
        Save a Pandas DataFrame to a CSV file.

        Parameters
        ----------
        - data_frame (pd.DataFrame): Pandas DataFrame to be saved.
        - file_path (str): File path for saving the CSV file.

        Returns
        -------
        - None
        """
        data_frame.to_csv(file_path, index=False)


if __name__ == "__main__":
    db_connector = RDSDatabaseConnector()

    # Step 1: Extract loan data from the RDS database
    loan_query = "SELECT * FROM loan_payments"
    loan_data = db_connector.extract_data_to_dataframe(loan_query)

    # Step 2: Save loan data to a local CSV file
    db_connector.save_to_csv(loan_data, 'loan_data.csv')


