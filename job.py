import json 
import os 
import logging 
import sys 
import pandas as pd 
from typing import Optional 
from classes import SparkWorker



class WarehouseWorker:
    def __init__(self,
                 config_filepath:Optional[str]=None,
                 run_fetch_loop:Optional[bool]=False,
                 fetch_data_with_creation:Optional[bool]=False,
                 warehouse_day_length:Optional[int]=86400,
                 warehoused_data_dir:Optional[str]="warehoused_data",
                 data_filepath:Optional[str]="warehoused_event_data.json",
                 batch_size:Optional[int]=10,
                 spark_config_filname:Optional[str]="spark_config.json"):
        
        """A job based class for the use of interpreting DataWarehouser actions.
           NOTE: methods will be in upper-case for global job scope implication.
          :param: config_filepath : api key credential filepath(denotes that you are fetching)
          :param: run_fetch_loop : instantiate the DataWarehouser and run on a 24-hour sleep timer"""
        
        self.project_dir = self.ESTABLISH_LOGGING()
        self.warehouse_day_length = warehouse_day_length  
        self.warehoused_data_dir = warehoused_data_dir
        self.batch_size = batch_size
        self.warehouse_day_length = warehouse_day_length
        self.REFRESH_DATAFRAMES()
        if config_filepath is not None:
            self.config_filepath = config_filepath  
        if run_fetch_loop:
            self.RUN_FETCH() 
        elif fetch_data_with_creation:
            self.warehouser = self.MAKE_DataWarehouser(login_filepath=self.config_filepath, fetch_data=True) 
        else:
            self.warehouser = self.MAKE_DataWarehouser(login_filepath=self.config_filepath,  fetch_data=False)
        self.warehoused_data_filepath = os.path.join(self.project_dir, self.warehoused_data_dir, data_filepath)
        self.working_df = self.MAIN()
        self.spark_worker = self.MAKE_SparkWorker()
        self.spark_session = self.spark_worker.spark 
        

    def REFRESH_DATAFRAMES(self):
        if os.path.exists(self.warehoused_data_dir):
            for f in os.listdir(self.warehoused_data_dir):
                f = str(os.path.join(self.project_dir, self.warehoused_data_dir, f))
                if f.endswith("v"):
                    os.remove(f)
                else:
                    pass 
                
    
    def RUN_FETCH(self):
        from time import sleep 
        while run_fetch:
            try:
                self.MAKE_DataWarehouser(self.config_filepath)
                sleep(self.warehouse_day_length)
            except ConnectionError as e:
                print(e)
                run_fetch = False 
        

    def ESTABLISH_LOGGING(self):
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        project_dir = os.path.join(parent_dir, "roseChain")
        log_dir = os.path.join(project_dir, "logs")
        LOG_FILE = os.path.join(log_dir, f"job-{os.path.basename(__file__)}.log")
        LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
        logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT, filemode="w")
        logger = logging.getLogger()
        sys.path.insert(1, project_dir)
        return project_dir

                    
    def MAKE_DataWarehouser(self, login_filepath, fetch_data):
        from classes import DataWarehouser
        return DataWarehouser(config_filepath=login_filepath, 
                              batch_size=self.batch_size, fetch_data=True) if fetch_data \
            else DataWarehouser(config_filepath=login_filepath, batch_size=self.batch_size)
    
    
    def RETRIEVE_ALL_EVENTS_BY_COUNTRY(self, country="US"):
        return self.warehouser._GET_events_per_country(country)
    
    
    def MAIN(self, dirs_to_clear=["spark_df", "spark-warehouse", "warehoused_data"]):
        return self.warehouser.main_data_warehousing()
    
    
    def MAKE_SparkWorker(self):
        return SparkWorker()
######################END WORKER CLASS#############################


def EXECUTE_JOB(fetch:bool, sleep_time:int, validation_attempts:int=3):
    from time import sleep 
    push_to_pg = True if fetch else False 
    run = 1 
    while run <= validation_attempts:
        try:
            WORKER = WarehouseWorker(config_filepath="login.json", fetch_data_with_creation=fetch, batch_size=200)  
            SPARK_WORKER = WORKER.spark_worker
            df = SPARK_WORKER.construct_df(WORKER.working_df, push_df_to_postgres=push_to_pg, run_number=run)
            print(f"\033[94mJob number {run} completed! Sleeping for {sleep_time} seconds...\033[1m")
            sleep(sleep_time) 
            run += 1
        except ConnectionError as e:
            print(e)
            continue




from classes import DataParser
from time import sleep 
from os.path import join 
FETCH = True
SLEEP_TIME = 5
VALIDATION_ATTEMPTS = 1 
DATA_DIRNAME = "warehoused_data"
ATTRACTION_DATA_FILENAME = "attraction_data_warehoused_event_data.json"
EVENT_DATA_FILENAME = "event_data_warehoused_event_data.json"
if __name__ == "__main__":
    dataparser = DataParser(fetch_data=True)
    attraction_df = dataparser.parse_attraction_data(ATTRACTION_DATA_FILENAME, 200)
    event_df = dataparser.parse_event_data(EVENT_DATA_FILENAME, 200)
    event_df.to_csv(join(DATA_DIRNAME, "events_df.csv"))
    attraction_df.to_csv(join(DATA_DIRNAME, "attractions_df.csv"))
    print(f"Job done with {VALIDATION_ATTEMPTS} validation attempts! Sleeping until tomorrow...")
    
    
    
        

