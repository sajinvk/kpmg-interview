import pandas as pd 
import logging_class
import json
import sys 

class SaveOutput:
    
    log_file_name = 'etl_log.out'
    config_file = 'config_data.json'
    log = logging_class.setup_logging(log_file_name)
    
   
    
    def __init__(self , v_dataframe , v_out_name  ):
        
        try:
            
            self.dataframe = v_dataframe
            self.name = v_out_name
            self.log.logger.info('Get Argument for destination')
            self.config_data =  json.load(open(self.config_file))
           
            for dest, desttype in self.config_data['out_data_type'].items():
                         
                if dest == 'operating_system':
                    self.v_format_type = desttype['format']
                    self.log.logger.info('Output logging format')
                    self.log.logger.info(self.v_format_type)
                
        except:
            self.log.logger.error('Cannot read Configuration file : config.json')
            self.log.logger.error(sys.exc_info()[0])
        
    def save_data (self):
        
        try:
            self.log.logger.info('Saving file : ' + self.name)

            if self.v_format_type == 'csv' :
                self.dataframe.to_csv(self.name)
            elif self.v_format_type == 'json' :  
                self.dataframe.to_json(self.name)
            elif self.v_format_type == 'parquet' :
                self.dataframe.to_parquet(self.name)
            else:
                self.log.logger.error('Wrong format specified .Check config file')
            
        except:   
            self.log.logger.error('Error Saving file ')
            self.log.logger.error(sys.exc_info()[0])
            
