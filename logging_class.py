import logging 

class setup_logging:
    """
     Setup standard Logging format in debug or info mode 
     Default : Info mode . To change to debug , Look for comment Change_to_Debug
     Ouput File_Name = Pass in as a parameter 
     
    """
    def __init__(self, out_file_name):    

        try:
            debug = 0   # Change_to_Debug . Set to 1 for debug mode 
            if debug == 1 :
               info = logging.DEBUG
            else :
               info = logging.INFO
            self.logger = logging.getLogger()
            self.logger.setLevel(info)
            #File handler
            handler = logging.FileHandler(out_file_name)
            handler.setLevel(info)
            # Format
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            # Handlers to the logger
            self.logger.addHandler(handler)
            
        except IOError:
            self.logger.error("Error opening Log file .Check write permission in the folder ",exc_info=True)
