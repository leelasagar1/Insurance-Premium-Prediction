
import pandas as pd
import numpy as np
import os
import sys
import yaml
from insurance.exception import InsuranceException
from insurance.logger import logging
from insurance.config import mongo_client


def get_collection_as_dataframe(database_name:str,collection_name:str) -> pd.DataFrame:

    try:
        logging.info(f'Reading Data from database: {database_name} and collection: {collection_name}')

        df = pd.DataFrame(mongo_client[database_name][collection_name].find())

        logging.info(f'columns:{df.columns}')

        if '_id' in df.columns:

            logging.info("Dropping _id columns")

            df.drop('_id',axis=1,inplace=True)

        logging.info(f"Row and columns:{df.shape}")
        return df

    except Exception as e:

        raise InsuranceException(e,sys)
    

def convert_columns_float(df:pd.DataFrame, exclude_columns:list) -> pd.DataFrame:

    try:
        for column in df.columns:
            
            if column not in exclude_columns and df[column].dtype != 'O':

                df[column] = df[column].astype('float')
                
        return df 

    except Exception as e:

        raise InsuranceException(e,sys)
    

def write_yaml_file(file_path,data:dict):

    try:
        file_dir = os.path.dirname(file_path)

        os.makedirs(file_dir,exist_ok=True)

        with open(file_path,'w') as file_write:

            yaml.dump(data,file_write)

    except Exception as e:

        raise InsuranceException(e,sys)
            
