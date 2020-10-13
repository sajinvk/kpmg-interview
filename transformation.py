import pandas as pd 
import logging_class
import pandasql as ps
from urllib.parse import urlparse , parse_qs
import sys 

class Transformation:
    """
    Multiple Transformations 
      1. Convert time column in to a human readable format. 
      2. Extract medium ,source and path from URL.
      3. List Top count medium and source.
      4. List distinct users count, min time , max time for a subset of records.
      5. LIst distinct users count by day. 
    
    """
    log_file_name = 'etl_log.out'
    log = logging_class.setup_logging(log_file_name)
    #pysqldf = lambda q: ps.sqldf(q, globals())
    
    def __init__(self , source_df ):
        
        self.source_df = source_df
     
    def set_time_readable(self):
        
        try:
            self.log.logger.info('start: Change column time to human readable format- ')
            self.source_df['time']=(pd.to_datetime(self.source_df['time'],unit='s'))
            self.log.logger.info(' Success: Change column time to human readable format')
        except:
            self.log.logger.error("Error executing set_time_readable " )
            self.log.logger.error(sys.exc_info()[0])
            
                
    def runSQL(self , v_sql):
        """
        execute sql and return dataframe using pandassql 
        """
        try:
            df_out = ps.sqldf(v_sql)
            self.log.logger.info("Executed SQL successfully " +v_sql )
            return df_out 
        
        except:
            self.log.logger.error("Error executing SQL query " +v_sql)
            self.log.logger.error(sys.exc_info()[0])
            
        
    def get_medium(self, x):
        """
        Extract Medium using urlparse to extract Medium - look for string 'utm_medium'
        Return multiple medium as one string appended using separator '---'
        """
        try:
            v_medium = 'utm_medium'
            if v_medium in parse_qs(urlparse(x['url']).query).keys():
                v_medium_list = parse_qs(urlparse(x['url']).query)['utm_medium']
                v_medium_set = set(v_medium_list)
                v_uniq_medium_list = list(v_medium_set)

                v_ret = None
                for i in range (len(v_medium_set)):
                        if i == 0:
                            v_ret = v_uniq_medium_list[0]
                        else:
                            v_ret = v_ret+ "---" +v_uniq_medium_list[i]
                return v_ret 
            
                #return (v_medium_list[0])

            else: 
                return None 
        except:
            self.log.logger.error("Error executing get_medium function")
            self.log.logger.error(sys.exc_info()[0])
             
    
    def get_source(self, x):

        """
        Extract Source using urlparse to extract Soruce - look for string 'utm_source'
        Return multiple medium as one string appended using separator '---'
        """
        try:
            v_source = 'utm_source'
            if v_source in parse_qs(urlparse(x['url']).query).keys():
                v_source_list = parse_qs(urlparse(x['url']).query)['utm_source']
                v_source_set = set (v_source_list)
                v_uniq_source_list = list(v_source_set)
                v_ret = None 
                for i in range (len(v_uniq_source_list)):
                    if i == 0:
                        v_ret = v_uniq_source_list[0]
                    else:
                        v_ret = v_ret+ "---" +v_uniq_source_list[i]
                return v_ret 


            else: 
                return None 
        
        except:  
            self.log.logger.error("Error executing get_source function")
            self.log.logger.error(sys.exc_info()[0])
             



    def get_path(self , x):
        """
        URL will always return path , unless wrong data is captured . Blank URL will have a string :'/'
        """
        try:
            if 1:
                return urlparse(x['url']).path
        
        except:
            self.log.logger.error("Error executing get_source function")
            self.log.logger.error(sys.exc_info()[0])
             
            
        
    def set_medium_source_path(self):
        """
        Modify soruce Data frame. Add 3 columns 
        """
        try:
            self.log.logger.info('start: Extract Medium')
            self.source_df['utm_medium'] = self.source_df.apply(self.get_medium, axis =1)
            self.log.logger.info('End : Extract Medium')
            
            self.log.logger.info('start: Extract Source')
            self.source_df['utm_source'] = self.source_df.apply(self.get_source, axis =1)
            self.log.logger.info('End : Extract Source')
            
            self.log.logger.info('start: Extract Path')
            self.source_df['path'] = self.source_df.apply(self.get_path, axis=1)
            self.log.logger.info('End : Extract Path')
            
                   
        except:
            self.log.logger.error("Error executing set_medium_source function")
            self.log.logger.error(sys.exc_info()[0])
      
        
    def final_dataframe(self):
        """
        Capture first entry of medium and source. Subsequent entry is not considered as the result 
        in normalized SQL database 
            #1. list top medium and source 
            #2. Clean up records with null values 
            #3  Exclude all columns except medium and source 
            #4. Count of distinct utm_medium and utm_source   
        """
        try:
            #top_source_medium_df = []          
            top_source_medium_sql = """
            select utm_medium , utm_source , count(*) top_count
            from source_df 
            where 
            utm_medium is not Null 
            and utm_source is not  Null 
            group by utm_medium , utm_source 
            order by top_count desc """

            #top_source_medium_df = self.runSQL(top_source_medium_sql)
            self.log.logger.info('Start : Distinct cout of UTM SOURCE and MEDIDUM')
            drop_null_df = self.source_df.dropna()
            top_source_medium_df = drop_null_df.groupby(['utm_medium','utm_source']).size().reset_index(name='counts')
            top_source_medium_df = top_source_medium_df.sort_values(['counts'], ascending = False)
            #print(top_source_medium_df)
            self.log.logger.info('End : Distinct cout of UTM SOURCE & MEDIDUM')
            
            return top_source_medium_df
       
        except:
            self.log.logger.error("Error executing final_dataframe function")
            self.log.logger.error(sys.exc_info()[0])
            

        

    def calculate_metrics(self , in_sliced_df ):
        """
        Distinct users in the set of rec_count records 
        Minimum time & Maximum Time - Earliest and Last login Time 
        This is run for every subset of  records in the sliced dataframe
        """

        try:
            
            min_time = min(in_sliced_df['time'])
            max_time = max(in_sliced_df['time'])
           # distinct_count_userid = len(in_sliced_df.nunique(axis= 'anonymous_user_id'))
            distinct_count_userid = in_sliced_df['anonymous_user_id'].nunique()
            #print(min_time)

            return min_time, max_time , distinct_count_userid
        
        except:
            self.log.logger.error("Error executing calculate_metrics function")
            self.log.logger.error(sys.exc_info()[0])
    
    def calc_distinctusers_perday(self ):
        """
        Calculate distinct users per day 
        """

        try:
            self.log.logger.info("Start :Function: calc_distinctusers_perday")
            #self.log.logger.info(self.source_df.head(1))
            self.source_df['time'] = self.source_df['time'].dt.date
            grouped_df = self.source_df.groupby('time')
            grouped_df = grouped_df.agg({'anonymous_user_id': 'nunique'})
            grouped_df = grouped_df.reset_index()
            #print(grouped_df)
            self.log.logger.info("End : calc_distinctusers_perday")
            return grouped_df
          
        
        except:
            self.log.logger.error("Error executing calc_distinctusers_perday function")
            self.log.logger.error(sys.exc_info()[0])
            

    def execute_metrics (self , subset_count):
        """
        Execute Calculate metrics on ordered dataframe 
        """ 
        try:
            
            orderby_final_df= self.source_df.sort_values(by=['time'])
            col_names = ["min_time" , "max_time", "count_distinct_id"]
            df_metrics_out = pd.DataFrame(columns = col_names)
            self.log.logger.info("Start : Function execute_metrics")

            for i in range(len(orderby_final_df)):
                if ((i+1)%subset_count) == 0 :
                    v_end = i 
                    v_start = i - (subset_count -1)
                    sliced_df = orderby_final_df[v_start:v_end]
                    min_time, max_time, distinct_count = self.calculate_metrics(sliced_df)
                    df_metrics_out = df_metrics_out.append(pd.DataFrame([[min_time, max_time, distinct_count]] , columns = df_metrics_out.columns))
            self.log.logger.info("End : Function execute_metrics")           
            return  df_metrics_out  
        
        except:
            self.log.logger.error("Error executing execute_metrics function")
            self.log.logger.error(sys.exc_info()[0])
            
