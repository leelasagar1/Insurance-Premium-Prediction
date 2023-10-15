from insurance.entity import artifact_entity, config_entity
from insurance.exception import InsuranceException
from insurance.logger import logging
from insurance.config import TARGET_COLUMN
from insurance import utils 

import os,sys
import pandas as pd
from typing import Optional
from scipy.stats import ks_2samp
import numpy as np


class DataValidation:

    def __init__(self,data_validation_config: config_entity.DataValidationConfig, 
                 data_ingestion_artifact: artifact_entity.DataIngestionArtifact):
        
        try:
            logging.info("************** Data Validation ******************")
            self.data_validation_config = data_validation_config
            self.data_ingestion_artifact = data_ingestion_artifact
            self.validation_error = dict()

        except Exception as e:

            raise InsuranceException(e,sys)
        
    # to drop missing values columns
    def drop_missing_values_columns(self,df:pd.DataFrame,report_key_name:str) -> Optional[pd.DataFrame]:
        
        try:
            threshold = self.data_validation_config.missing_threshold
            null_report = df.isna().sum() / df.shape[0]
            drop_columns = null_report[null_report>threshold].index

            #stores all dropped columns
            self.validation_error[report_key_name ] = list(drop_columns)

            df.drop(list(drop_columns),axis=1,inplace=True)

            if len(df.columns)==0:
                return None
            

            return df
        except Exception as e:

            raise InsuranceException(e,sys)
        
    #check if all the required column present in the dataset
    def is_required_columns_exits(self, base_df:pd.DataFrame,current_df:pd.DataFrame,report_key_name:str) -> bool:
        
        try:
            base_columns = base_df.columns
            current_columns = current_df.columns

    
            missing_columns = []

            for base_column in base_columns:

                if base_column not in current_columns:
                    
                    logging.info(f'{base_column} column is not available')

                    missing_columns.append(base_column)

            if len(missing_columns)>0:
                
                #stores missing columnss
                self.validation_error[report_key_name] = missing_columns

                return False
            return True
                
        except Exception as e:

            raise InsuranceException(e,sys)  

    #check if distribution of new data matches the old data distribution
    def data_drift(self,base_df:pd.DataFrame,current_df:pd.DataFrame,report_key_name:str):

        try:
            drift_report = dict()

            base_columns = base_df.columns
            current_columns = current_df.columns

            for base_column in base_columns:
                
                #check for distribution change
                same_distribution = ks_2samp(base_df[base_column],current_df[base_column])

                if same_distribution.pvalue > 0.05:

                    drift_report[base_column] = {
                        'pvalues': float(same_distribution.pvalue),
                        'same_distribution': True
                    }
                else:

                    drift_report[base_column] = {
                        'pvalues': float(same_distribution.pvalue),
                        'same_distribution': False
                    }

            self.validation_error[report_key_name] = drift_report



        except Exception as e:

            raise InsuranceException(e,sys)  
        

    def initiate_data_validation(self) -> artifact_entity.DataValidationArtifact:
        
        try:
            
            #base data
            base_df = pd.read_csv(self.data_validation_config.base_file_path)
            base_df.replace({'na',np.NAN},inplace=True)
            base_df = self.drop_missing_values_columns(base_df,report_key_name='Missing_values_within_base_dataset')

            #train data
            train_df = pd.read_csv(self.data_validation_config.base_file_path)
            train_df = self.drop_missing_values_columns(train_df,report_key_name='Missing_values_within_base_dataset')

            #test data
            test_df = pd.read_csv(self.data_validation_config.base_file_path)
            test_df = self.drop_missing_values_columns(test_df,report_key_name='Missing_values_within_base_dataset')

            exclude_columns = [TARGET_COLUMN]

            base_df = utils.convert_columns_float(base_df,exclude_columns=exclude_columns)
            train_df = utils.convert_columns_float(base_df,exclude_columns=exclude_columns)
            test_df = utils.convert_columns_float(base_df,exclude_columns=exclude_columns)

            train_df_columns_status = self.is_required_columns_exits(base_df,train_df,report_key_name='Missing_values_within_train_dataset')
            test_df_columns_status = self.is_required_columns_exits(base_df,test_df,report_key_name='Missing_values_within_test_dataset')

            if train_df_columns_status:

                self.data_drift(base_df,train_df,report_key_name='data_drift_within_train_dataset')
            

            if test_df_columns_status:

                self.data_drift(base_df,test_df,report_key_name='data_drift_within_test_dataset')
            
            file_path = self.data_validation_config.report_file_path

            utils.write_yaml_file(file_path,data=self.validation_error )

            data_validation_artifact = artifact_entity.DataValidationArtifact(
                report_file_path= self.data_validation_config.report_file_path
            )

            return data_validation_artifact

        except Exception as e:

            raise InsuranceException(e,sys)  