import os, sys
from datetime import datetime
from insurance.exception import InsuranceException

FILE_NAME = 'insurance.csv'

TRAIN_FILE_NAME = 'train.csv'

TEST_FILE_NAME = 'test.csv'

TRANSFORMER_OBJECT_FILE_NAME = 'transformer.pkl'
TARGET_ENCODER_OBJECT_FILE_NAME = "target_encoder.pkl"


class TrainingPipelineConfig:
    
    def __init__(self):
        try:
            self.artifact_dir = os.path.join(os.getcwd(),'artifact',f"{datetime.now().strftime('%m%d%Y__%H%M%S')}")
        
        except Exception as e:

            raise InsuranceException(e,sys)
#
# to store data file paths and names
class DataIngestionConfig:

    def __init__(self,training_pipeline_config: TrainingPipelineConfig):

        try:
            self.database_name = 'INSURANCE'
            self.collection_name = 'INSURANCE_PROJECT'
            self.data_ingestion_dir = os.path.join(training_pipeline_config.artifact_dir,'data_ingestion')
            self.feature_store_file_path = os.path.join(self.data_ingestion_dir,'feature_store',FILE_NAME)
            self.train_file_path = os.path.join(self.data_ingestion_dir,'dataset',TRAIN_FILE_NAME)
            self.test_file_path = os.path.join(self.data_ingestion_dir,'dataset',TEST_FILE_NAME)
            self.test_size = 0.2


        except Exception as e:

            raise InsuranceException(e,sys)
        
    #convert data into Dict
    def to_dict(self) -> dict:

        try:
            return self.__dict__
        

        except Exception as e:

            raise InsuranceException(e,sys)
        

class DataValidationConfig:

    def __init__(self,training_pipeline_config:TrainingPipelineConfig):

        self.data_validation_dir = os.path.join(training_pipeline_config.artifact_dir,'data_validation')
        self.report_file_path = os.path.join(self.data_validation_dir,'report.yaml')
        self.missing_threshold = 0.20
        self.base_file_path = os.path.join("data/insurance.csv")
        

class DataTransformationConfig:

    def __init__(self,training_pipeline_config:TrainingPipelineConfig):

        self.data_transformation_dir = os.path.join(training_pipeline_config.artifact_dir,'data_transformation')
        # path to save transformation pipeline
        self.transform_object_path = os.path.join(self.data_transformation_dir,'transformed','TRANSFORMER_OBJECT_FILE_NAME')
        self.transform_train_path = os.path.join(self.data_transformation_dir,'transformed',TRAIN_FILE_NAME.replace('csv','npz'))
        self.transform_test_path = os.path.join(self.data_transformation_dir,'transformed',TEST_FILE_NAME.replace('csv','npz'))

        #path to save encoder 
        self.target_encoder_path = os.path.join(self.data_transformation_dir,"target_encoder",TARGET_ENCODER_OBJECT_FILE_NAME)
