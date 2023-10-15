from insurance.logger import logging
from insurance.exception import InsuranceException

from insurance.utils import get_collection_as_dataframe
from insurance.entity.config_entity import (TrainingPipelineConfig,
                                            DataIngestionConfig,
                                            DataValidationConfig,
                                            DataTransformationConfig)
from insurance.entity import config_entity  
from insurance.components.data_ingestion import DataIngestion
from insurance.components.data_validation import DataValidation
from insurance.components.data_transformation import DataTransformation

import os
import sys

def test_logger_and_exception():

    try:
        # get_collection_as_dataframe(database_name='INSURANCE',collection_name='INSURANCE_PROJECT')

        training_pipeline_config = TrainingPipelineConfig()

        #data ingestion
        #contains all the file_paths
        data_ingestion_config = DataIngestionConfig(training_pipeline_config)

        data_ingestion = DataIngestion(data_ingestion_config)
        # returns all data paths
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()

        #data validation
        #contains all the file names and paths
        data_validation_config = DataValidationConfig(training_pipeline_config)
        
        data_validation = DataValidation(data_validation_config,data_ingestion_artifact)

        #start data validation
        data_validation_config = data_validation.initiate_data_validation()

        #data transformation
        data_transformation_config = DataTransformationConfig(training_pipeline_config)

        data_transformation = DataTransformation(data_transformation_config,data_ingestion_artifact)

        data_transformation_artifact = data_transformation.initiate_data_transformation()


        
    except Exception as e:

        logging.debug(str(e))
        raise InsuranceException(e,sys)
    

if __name__ == "__main__":

    test_logger_and_exception()