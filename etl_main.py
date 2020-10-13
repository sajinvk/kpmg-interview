import argparse
from get_data import getdata
from transformation import Transformation
from load_output import SaveOutput

def GetArgs():
        """
        Supports the command-line arguments listed below.
        
        """
        parser = argparse.ArgumentParser(
           description='Reads one argument -t (TYPE (e.g csv , json , api ))')
        parser.add_argument('-t', '--input_file_type', action='store',default = "csv",
                                           help='FILE TYPE.')
        args = parser.parse_args()
        return args 

        
        
if __name__ == '__main__':
   # v_args = GetArgs() 
   # v_input_file_type = v_args.input_file_type
    
    v_input_file_type = "csv"
    # Import Data : class get_data
    obj_get_data = getdata()
    source_df = obj_get_data.ReadCSVData()
    
    # Transformation : class transformation 
    
    obj_transformation = Transformation(source_df)
    obj_transformation.set_time_readable()
    obj_transformation.set_medium_source_path()
    top_source_medium_df = obj_transformation.final_dataframe()
    #v_name = top_source_medium_df.
    print(top_source_medium_df)
    distinctusers_batch = obj_transformation.execute_metrics(10000)  
    print(distinctusers_batch) 
    distinctusers_day = obj_transformation.calc_distinctusers_perday()
    print(distinctusers_day)
    
    # Save Output based on required format 
    
    obj_SaveOutput = SaveOutput(top_source_medium_df, 'top_source_medium')
    obj_SaveOutput.save_data()
    
    obj_SaveOutput = SaveOutput(distinctusers_batch, 'distinct_users_batch')
    obj_SaveOutput.save_data()
    
    obj_SaveOutput = SaveOutput(distinctusers_day, 'distinct_users_day')
    obj_SaveOutput.save_data()
    
