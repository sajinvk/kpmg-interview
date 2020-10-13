import pandas as pd 
import json 
import logging_class
import sys 


class getdata:
    """
    Read config file to get the name of source file 
    
    """
    log_file_name = 'etl_log.out'
    config_file = 'config_data.json'
    log = logging_class.setup_logging(log_file_name)
    
    def __init__(self ):
        
        try:
            self.log.logger.info('Get Argument for the Type of source')
            
            self.config_data =  json.load(open(self.config_file))
            #print(self.config_data)
            for dataSource, dataSet in self.config_data['data_type'].items():
                #print(dataSource)
                if dataSource == 'csv':
                    self.csv_file_key = dataSet['utm_data']
                    self.log.logger.info('Source File Details: ' + self.csv_file_key)
        except:
            self.log.logger.error('Cannot read Configuration file : config_data.json')
            self.log.logger.error(sys.exc_info()[0])
      
    def ReadCSVData(self):
        """
        Read CSV and Return Dataframe 
        
        """
        try:
            self.log.logger.info('Start Reading Source file: ' + self.csv_file_key )
            source_df = pd.read_csv(self.csv_file_key)
            self.log.logger.info('Reading Complete '  )
            return source_df
        except:
            log.logger.error('Error Reading source file: ' + self.csv_file_key )
            self.log.logger.error(sys.exc_info()[0])

    
    
    def databases(self):
        pass
    
    def api(self):
        pass
