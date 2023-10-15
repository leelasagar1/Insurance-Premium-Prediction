import pandas as pd
import numpy as np
import os,sys

from insurance.entity import config_entity,artifact_entity
from insurance.exception import InsuranceException
from insurance import utils
from insurance.logger import logging

from sklearn.model_selection import train_test_split


class DataIngestion:

    def __init__(self, data_ingestion_config:config_entity.DataIngestionConfig) -> None:
        
        try:
            self.data_ingestion_config = data_ingestion_config

        except Exception as e:

            raise InsuranceException(e,sys)
        
    def initiate_data_ingestion(self,) -> artifact_entity.DataIngestionArtifact:
        try:
            logging.info(f"Export collection data as DataFrame")
            df: pd.DataFrame = utils.get_collection_as_dataframe(
                database_name=self.data_ingestion_config.database_name,
                collection_name=self.data_ingestion_config.collection_name
            )

            logging.info(f"Save data in Feature Store")
            
            # Replace na with NAN
            df.replace(to_replace='na',value=np.NAN,inplace=True)

            feature_store_dir = os.path.dirname(self.data_ingestion_config.feature_store_file_path)

            #create feature store folder is not available
            os.makedirs(feature_store_dir,exist_ok=True)

            logging.info('Saving df to feature store folder')
            df.to_csv(path_or_buf=self.data_ingestion_config.feature_store_file_path,index=False,header=True)

            logging.info(' Splitting data into Train and test set')
            train_df,test_df = train_test_split(df,test_size=self.data_ingestion_config.test_size,random_state=1)

            train_data_dir = os.path.dirname(self.data_ingestion_config.train_file_path)

            os.makedirs(train_data_dir,exist_ok=True)

            logging.info('Saving train df to feature store folder')
            train_df.to_csv(self.data_ingestion_config.train_file_path,index=False,header=True)

            test_data_dir = os.path.dirname(self.data_ingestion_config.test_file_path)

            os.makedirs(test_data_dir,exist_ok=True)

            logging.info('Saving test df to feature store folder')
            test_df.to_csv(self.data_ingestion_config.test_file_path,index=False,header=True)

            data_ingestion_artifact = artifact_entity.DataIngestionArtifact(
                feature_store_file_path = self.data_ingestion_config.feature_store_file_path,
                train_file_path = self.data_ingestion_config.train_file_path,
                test_file_path = self.data_ingestion_config.test_file_path
            )

            return data_ingestion_artifact
        except Exception as e:
            raise InsuranceException(e,sys)


