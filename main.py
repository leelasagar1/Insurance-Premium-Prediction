from insurance.logger import logging
from insurance.exception import InsuranceException

from insurance.utils import get_collection_as_dataframe
from insurance.entity.config_entity import TrainingPipelineConfig,DataIngestionConfig,DataValidationConfig
from insurance.entity import config_entity  
from insurance.components.data_ingestion import DataIngestion
from insurance.components.data_validation import DataValidation

import os
import sys

def test_logger_and_exception():

    try:
        # get_collection_as_dataframe(database_name='INSURANCE',collection_name='INSURANCE_PROJECT')
        training_pipeline_config = TrainingPipelineConfig()
        data_ingestion_config = DataIngestionConfig(training_pipeline_config)

        data_ingestion = DataIngestion(data_ingestion_config)
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()

        data_validation_config = DataValidationConfig(training_pipeline_config)

        data_validation = DataValidation(data_validation_config,data_ingestion_artifact)
        data_validation_config = data_validation.initiate_data_validation()
        
    except Exception as e:

        logging.debug(str(e))
        raise InsuranceException(e,sys)
    

if __name__ == "__main__":

    test_logger_and_exception()