#let us try an event specific search:
#Ticket master api requests have the following format:
    #https://app.ticketmaster.com/{package}/{version}/{resource}.json?apikey=**{API key}
    #for example: https://app.ticketmaster.com/discovery/v1/events.json?apikey=4dsfsf94tyghf85jdhshwge334



import json
from typing import Optional
import os 
import requests


class TicketmasterCursor:
    
    def __init__(self,
                 config_filepath=None,
                 fetch_data=False,
                 warehoused_data_dir="warehoused_data",
                 batch_size:Optional[int]=20,
                 consumer_key:Optional[str]=None,
                 consumer_secret:Optional[str]=None,
                 json_filepath:Optional[str]="/Users/alex/Desktop/roseChain/warehoused_data/warehoused_event_data.json",
                 name_filter:Optional[str]=None,
                 fetchby_events=False,
                 fetchby_attractions=False):
        
        self.api_url = 'https://app.ticketmaster.com/'
        self.warehoused_data_dir = warehoused_data_dir
        assert batch_size <= 200 
        self.batch_size = batch_size
        if config_filepath is None:
            self.consumer_key = consumer_key
            self.consumer_secret = consumer_secret
        if fetchby_events:
            api_call_string = self.generate_api_call_string(config_filepath, event_name=name_filter)
        elif fetchby_attractions:
            api_call_string = self.generate_api_call_string(config_filepath, resource="attractions.json?")
        if fetch_data:
            self.data_dict = self.execute_request_to_json(api_call_string, self.warehoused_data_dir)
        else:
            with open(json_filepath) as f:
                self.data_dict = json.load(f)
        #parse out df by feature?
           
           
    def _read_config(self, json_filepath):
        with open(json_filepath) as f:
            data = json.load(f)
        key, secret = data.values()
        return (key, secret)
    
    
    def generate_api_call_string(self, config_filepath=None, batch_size=None, package="discovery", version="v2", 
                                 resource="events.json?", event_name=None, api_key:Optional[str]=None):
        url = os.path.join(self.api_url, package, version, resource)
        if batch_size is None:
            batch_size = self.batch_size
        size = f"size={str(batch_size)}"
        if event_name is not None:
            url = url + f"&keyword={event_name}"
        url = url + size + "&apikey="
        if config_filepath is None:
            config = api_key
        elif config_filepath is None and api_key is None:
            config = self.consumer_key
        else:
            config = self._read_config(config_filepath)
        url = url + config[0]
        return url 
    #/discovery/v2/attractions.json?size=1&apikey=TinnDOaeOYCxYJOJo1vPIuNuzZqp6pWy
    
    
    def execute_request_to_json(self, api_call_string, data_dir, json_filename="warehoused_event_data.json"):
        raw_data = requests.get(api_call_string)
        data_dict = raw_data.json()
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
        with open(os.path.join(data_dir, json_filename), 'w') as f:
            json.dump(data_dict, f, indent=4)
        return data_dict
    
    
    def _GET_events_per_country(self, country="US"):
        key = self._read_config("login.json")[0]
        url = f"https://app.ticketmaster.com/discovery-feed/v2/events.json?apikey={key}&countryCode={country}"
        raw_event_request = requests.get(url)
        raw_event_data_dict = raw_event_request.json()
        with open(os.path.join(self.warehoused_data_dir, f"all{country}_events.json"), "w") as f:
            raw_data_dict = json.dump(raw_event_data_dict, f, indent=4)
        return raw_data_dict 
##################END TICKETMASTERCURSOR CLASS@########################## 

import pandas as pd 

tm = TicketmasterCursor(config_filepath="login.json", batch_size=200, fetch_data=False, fetchby_attractions=True)
with open("warehoused_data/warehoused_event_data.json", "r") as fp:
    data_list = json.load(fp)["_embedded"]["attractions"]
working_data_dict = {}
for i, event in enumerate(data_list):
    working_data_dict[i+1] = event
extract = lambda f: [e[f] for e in working_data_dict.values()]
dataseed = {"attraction_id":extract("id"), "attraction_name":extract("name"), 
            "total_upcoming_attraction_events":[e["_total"] for e in extract("upcomingEvents")],
            "upcoming_attraction_ticketmaster_events":[e["ticketmaster"] if "ticketmaster" in e.keys() \
                                                                         else "NA" for e in extract("upcomingEvents")],
            "attraction_genre":[t[0]["genre"]["name"] for t in extract("classifications")]}
df = pd.DataFrame(dataseed)
print(df.head())

    



import pandas as pd 
from os.path import join 


class DataWarehouser(TicketmasterCursor):
    
                          #dialect      #username #pwd    #hostname #portid  #db_name
    __connection_string = 'postgresql://postgres:07141989@localhost:5432'
    
    def __init__(self,
                 config_filepath=None,
                 warehoused_data_dir="warehoused_data",
                 warehoused_data_filename="warehoused_event_data.json",
                 batch_size=20,
                 consumer_key=None,
                 consumer_secret=None,
                 fetch_data=False,
                 connection_details=__connection_string,
                 database_name="roseChain_working_data"):
        
        super().__init__(config_filepath=config_filepath, consumer_key=consumer_key, 
                         consumer_secret=consumer_secret, warehoused_data_dir=warehoused_data_dir,
                         batch_size=batch_size, fetch_data=fetch_data) 
        warehoused_data_filepath = join(self.warehoused_data_dir, warehoused_data_filename)
        self.raw_data_dict = self._readin_raw_data_dict(warehoused_data_filepath)
        self.working_data_list = self._generate_working_data_list(self.raw_data_dict)
        self.working_df_save_filepath = os.path.join(self.warehoused_data_dir, "working_data.csv")
        connection_string = os.path.join(connection_details, database_name)
        #web browser spark connection: http://localhost:4041/jobs/
        
       
       
    def _get_data_filelist(self, data_dir):
        filelist = []
        for root, _, files in os.walk(data_dir):
            for f in files:
                single_file = os.path.join(root, f)
                filelist.append(single_file)
        return filelist 
    
    
    def _extract_outer_feature_indexes(self, feature_name, working_data_list):
        """for reg features in the [1]th axis"""
        events = []
        for i, event in enumerate(working_data_list):
            if feature_name in event.keys():
                events.append(i)    
        return events 

    
    def _extract_inner_feature_indexes(self, working_data_list, inner_key:str, outer_key:str):
        """for example: if "presales" in event["sales"].keys()...within the [2]th axis 
                         #innerkey           #outerkey"""
        events = []
        for i, event in enumerate(working_data_list):
            if inner_key in event[outer_key].keys():
                events.append(i)
        return events
    
    
    def _parse_out_events(self, data_list, raw_data_dict, dirname="working_data_by_event", i=0):
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        while i < len(data_list):
            fp = os.path.join(dirname, f"event{i}_working_data.json")
            data = data_list[i]
            with open(fp, "w") as f:
                json.dump(data, f, indent=4) 
            i += 1
        raw_path = os.path.join("warehoused_data", "_embedded_RAW_DATA.json") #json is drilled into 
        inner_raw_data = raw_data_dict["_embedded"]["events"][0]
        with open(raw_path, "w") as FILE:
            json.dump(inner_raw_data, FILE, indent=4)
        return inner_raw_data  
    
    
    def _readin_raw_data_dict(self, filepath:Optional[str]=None):
        if filepath is None:
            filepath = "warehoused_data/warehoused_event_data.json"
        with open(filepath, "r") as f:
            raw_data = json.load(f)
        return raw_data
        
    
    def _generate_working_data_list(self, raw_data_dict, dim1_key="_embedded", dim2_key="events"):
        return raw_data_dict[dim1_key][dim2_key]
    
    
    def _access_df_column(self, df:pd.DataFrame, column_index=0):
        return df.iloc[:, column_index]


    def construct_single_event_dict(self, event_index:Optional[int]=0, single_event_dirname="working_data_by_event", save_json:bool=False):
        single_event_dict = self.working_data_list[event_index] #indexing working_data will yield a dictionary of event[i] where each feature is listed
        if save_json:
            with open(join(single_event_dirname, f"single_event{event_index}.json"), "w") as fp:
                json.dump(single_event_dict, fp, indent=4)
        return single_event_dict
    
    
    def _extract_outer_feature(self, feature_name:str, datalist):
        feature_list = []
        priceRanges_null = {"type":"NA", "currency":"NA", "min":0, "max":0}
        for i, event in enumerate(datalist):
            if type(event) == list:
                for v in event:
                    feature_list.append(v[feature_name])
            elif feature_name in event.keys():    
                feature_list.append(event[feature_name])
            else:
                feature_list.append(priceRanges_null)
        return feature_list
    
    
    def _parse_out_price_range_cols(self, priceRange_list, df):
        types = []
        currencies = []
        mins = []
        maxes = []
        for i, event in enumerate(priceRange_list):
            if type(event) == list: #make event a dict if trapped in list
                event = event[0]
            _df = pd.DataFrame(data=event, index=[0])
            types.append(event["type"])
            currencies.append(event["currency"])
            mins.append(event["min"])
            maxes.append(event["max"])
        df["trans_type"] = types 
        df["trans_currency"] = currencies
        df["trans_min"] = mins
        df["trans_max"] = maxes 
        return df
    
    
    def _parse_out_public_sales(self, existing_df, data_list=None, startDate_colname="startDateTime", endDate_colname="endDateTime"):
        if data_list is None:
            data_list = self.working_data_list
        start = []
        end = []
        null = {'startDateTime':'NA', 'endDateTime':'NA', 'startTBD':'NA'}
        for e in data_list:
            x = e["sales"]["public"]
            y = pd.DataFrame(x, index=[0])
            if startDate_colname in x.keys() and endDate_colname in x.keys():
                start.append([e[1] for e in y[startDate_colname].items()])
                end.append([e[1] for e in y["endDateTime"].items()])
            else:
                start.append(null)
                end.append(null)
        existing_df["public_sale_start"] = start
        existing_df["public_sale_end"] = end
        return existing_df
    
    
    def _parse_out_presales(self, data_list=None):
        if data_list is None:
            data_list = self.working_data_list
        presale_dict = {}
        presale = []
        null = {'name':'NA', 'description':'NA', 'url':'NA', 'startDateTime':'NA', 'endDateTime':'NA'}
        for i, event in enumerate(self.working_data_list):
            if "presales" in event["sales"].keys():
                presale_dict[event["name"]] = event["sales"]["presales"]
                presale.append(event["sales"]["presales"])
            else:
                presale_dict[event["name"]] = null
                presale.append(null)
        return presale
    
    
    def _drill_into_inner_presale_dicts(self, presale_list, existing_df):
        start = []
        end = []
        multi_presales = []
        presale_name = []
        for i, x in enumerate(presale_list):
            if type(x) == list:
                for v in x:
                    x = v 
                    z = {i:x}
                    multi_presales.append(z)
            y = pd.DataFrame(x, index=[0])
            start.append([e[1] for e in y["startDateTime"].items()])
            end.append([e[1] for e in y["endDateTime"].items()])
            presale_name.append([e[1] for e in y["name"].items()])
        existing_df["presale_start"] = start
        existing_df["presale_end"] = end
        existing_df["presale_name"] = presale_name
        return existing_df
    
    
    def _parse_out_event_date(self, data_list, existing_df):
        event_dates = []
        for event in data_list:
            if type(event) == list:
                event = event[0]
            data = event["dates"]["start"]
            if "dateTime" not in data.keys():
                event_dates.append("NA")
            else:
                event_dates.append(data["dateTime"])
        existing_df["event_date"] = event_dates  
        return existing_df
    
    
    def _extract_feature_to_series(self, data_list, index, feature_name):
        return pd.Series([data_list[index][feature_name]], name=feature_name)
    
    
    def _construct_working_data_dataframe(self, data_list):
        working_data_df = pd.DataFrame()
        i = 0
        while i < len(data_list):
            _name_data = self._extract_feature_to_series(data_list, i, "name").to_frame()
            working_data_df = pd.concat([working_data_df, _name_data], ignore_index=True)
            i += 1 
        return working_data_df
    
    
    def _create_annotation_file(self, filename="working_data_annotations.json"):
        from ANNOTATIONS import WORKING_DATA_ANNOTATIONS as ANN 
        filepath = os.path.join(self.warehoused_data_dir, filename)
        if not os.path.exists(filepath):
            with open(os.path.join(self.warehoused_data_dir, filename), "w") as fp:
                json.dump(ANN, fp, indent=4)
    
    
    def _prepare_price_range_features(self, data_list, existing_df):
        price_ranges_list = self._extract_outer_feature("priceRanges", data_list)
        return self._parse_out_price_range_cols(price_ranges_list, existing_df)
    
    
    def read_taylor_swift(self, taylorSwift_data_filepath=None, save_df:bool=False):
        print('POOOOOOOOOOOOP!!!')
        with open(taylorSwift_data_filepath, "r") as f:
            raw_data = json.load(f) 
        if save_df:
            df = pd.DataFrame.from_dict(raw_data)
        return raw_data if not save_df else df 
    
    
    def _clean_date_cols(self, df):
        def clean(colname):
            for e in df[colname]:
                e = str(e).replace("[", "").replace("]", "")
            return df[colname]
        df["presale_start"] = df["presale_start"].astype(str)
        df["presale_end"] = df["presale_end"].astype(str)
        df["public_sale_start"] = df["public_sale_start"].astype(str)
        return df 
        
    
    def main_data_warehousing(self, DATA_LIST:Optional[list]=None):
        if DATA_LIST is None:
            DATA_LIST = self.working_data_list
        
        def __prepare_ids(df, data_list=DATA_LIST):
            ids = self._extract_outer_feature("id", data_list)
            df["tktmstr_id"] = ids
            return df 
        
        def __prepare_price_range_features(df, data_list=DATA_LIST):
            price_ranges_list = self._extract_outer_feature("priceRanges", data_list)
            return self._parse_out_price_range_cols(price_ranges_list, df)
        
        def __prepare_all_sales_features(df, data_list=DATA_LIST):
            sales_list = self._extract_outer_feature("sales", data_list)
            presale_list = self._parse_out_presales(sales_list)
            df = self._drill_into_inner_presale_dicts(presale_list, df)
            return self._parse_out_public_sales(df) 
        
        def __prepare_outer_object_dicts(df, outer_object_key=None, d1_key=None, d2_key=None, feature_colname=None, data_list=DATA_LIST):
            #preferred method of extracting objects from arrays and inner arrays
            def extract(outer_object_key, d1_key=None, d2_key=None):
                feature_list = []
                for i, event in enumerate(data_list):
                    if type(event) == list:
                        event = event[0]
                    try:
                        y = lambda l, x: l.append(event[x][0][d1_key]) if x in event.keys() else l.append("NA")
                        y(feature_list, outer_object_key)
                    except:
                        if type(event[outer_object_key]) == dict:
                            data = event[outer_object_key] 
                            for e in data:
                                if type(e) == list:
                                    e = e[0]
                                feature_list.append(e[d1_key][d2_key]["name"])
                return feature_list 
            df[feature_colname] = extract(outer_object_key, d1_key, d2_key)
            if outer_object_key == "classifications":
                parse = lambda dataframe, colname: [e["name"] for e in dataframe[colname]]
                df["classification_type"] = parse(df, "classification_type")
            return df 
            
        working_data_df = self._construct_working_data_dataframe(DATA_LIST) 
        working_data_df = __prepare_ids(working_data_df)
        working_data_df = __prepare_price_range_features(working_data_df)
        working_data_df = __prepare_all_sales_features(working_data_df)  
        working_data_df = __prepare_outer_object_dicts(working_data_df, "promoters", "name", "promoter")
        working_data_df = __prepare_outer_object_dicts(working_data_df, "classifications", d1_key="genre", d2_key="name", feature_colname="classification_type")
        working_data_df = self._date_cols_to_string(working_data_df)
        self._parse_out_event_date(DATA_LIST, working_data_df)
        working_data_df.to_csv(self.working_df_save_filepath) 
        self._create_annotation_file()
        return working_data_df
###################END_DATAWAREHOUSER###########################################



from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
import datetime 



class SparkWorker:
    
    def __init__(self, app_name="roseChain", 
                 jdbc_jarpath="/Users/alex/Desktop/postgresql-42.5.1.jar",
                 db_name=None, username="postgres", new_tablename=None):
        self.app_name = app_name
        config_filepath = self._create_configuration()
        self.config_dict = self._read_configuration_file(config_filepath)
        self.connection_url = self._extract_connection_url(self.config_dict)
        self.spark = self._create_spark(jdbc_jarpath, app_name)
        
    
    def _create_configuration(self, spark_config_filepath="spark_config.json") -> str:
        config = {"driver":"jdbc", "dialect":"postgresql", "hostname":"localhost", "port_id":"5432", "db_name":"roseChain",
                  "driver_type":"org.postgresql.Driver", "username":"postgres", "password":"07141989"}
        if not os.path.exists(spark_config_filepath):
            with open(spark_config_filepath, "w") as fp:
                json.dump(config, fp, indent=5)
        else:
            None 
        return spark_config_filepath
    
    
    def _read_configuration_file(self, spark_config_filepath="spark_config.json") -> dict:
        #jdbc:postgresql://localhost:5432/testing_" --> #driver:dialect://hostname:portId/db_name
        with open(spark_config_filepath, "r") as fp:
            return json.load(fp)
        
        
    def _extract_connection_url(self, config_dict):
        return config_dict["driver"]+":"+config_dict["dialect"]+"://"+config_dict["hostname"] \
                                    +":"+config_dict["port_id"]+"/"+config_dict["db_name"]
    
    
    def _generate_spark_context(self, app_name=None):
        _conf = SparkConf().setMaster(self.connection_url).setAppName(app_name)
    
    
    def _create_spark(self, jarpath, app_name):
        return SparkSession.builder.config("spark.jars", jarpath).master("local[*]").appName(app_name).getOrCreate()   
        

    def _fill_string_na(self, df):
        def fill(colname):
            for i, e in enumerate(df[colname]):
                if type(e) == str:
                    e = float(0) 
                    df[colname][i] = e 
                else:
                    pass
            return df
        fill("trans_min")
        fill("trans_max")
        return df
    
    
    def _check_fetch_year(self, fetch_year):
        from datetime import datetime 
        this_year = int(datetime.today().strftime("%Y-%m-%d %H:%M:%S")[:4])
        different_year = False 
        if fetch_year != this_year:
            different_year = True
            difference = this_year - int(fetch_year) 
            print(f"{difference} YEARS DIFFERENCE")
        return str(fetch_year + difference) if different_year else str(fetch_year)
    
    
    def _dateTime_cols_to_string(self, df):
        def fill(colname):
            col_vals = []
            for e in df[colname]:
                if type(e) == list:
                    e = str(e).replace("[", "").replace("]", "") 
                col_vals.append(e)
            df[colname] = col_vals
            return df 
        colnames = ["presale_start", "presale_end", "public_sale_start", "public_sale_end", "event_date"]
        for cols in colnames:
            fill(cols)
        return df 
        
        
    def construct_df(self, PANDAS_DF, push_df_to_postgres:bool=False, run_number=0, spark_df_dirname="spark_df"):
        PANDAS_DF = self._fill_string_na(PANDAS_DF)
        schema = StructType([StructField("name", StringType(), False),
                             StructField("tktmstr_id", StringType(), True),
                             StructField("trans_type", StringType(), True),
                             StructField("trans_currency", StringType(), True),
                             StructField("trans_min", FloatType(), True),
                             StructField("trans_max", FloatType(), True),
                             StructField("presale_start", StringType()),
                             StructField("presale_end", StringType()),
                             StructField("presale_name", StringType()),
                             StructField("public_sale_start", StringType()),
                             StructField("public_sale_end", StringType()),
                             StructField("promoter", StringType(), True),
                             StructField("classification_type", StringType(), True),
                             StructField("event_date", StringType(), True)])
        spark_df = self.spark.createDataFrame(PANDAS_DF, schema).coalesce(1)
        if push_df_to_postgres:
            year = self._check_fetch_year(2022)
            tablename = "warehoused_event_data_fetch_number_" + str(run_number) + "_" + year 
            self.spark.sql(f"DROP TABLE IF EXISTS {tablename};")
            spark_df.select("*").write.mode("overwrite").option("header", True).format("jdbc").option("url", self.connection_url)\
                    .option("driver", self.config_dict["driver_type"]).option("dbtable", tablename)\
                    .option("user", self.config_dict["username"]).option("password", self.config_dict["password"]).save()
            spark_df_dirname = os.path.join("warehoused_data", spark_df_dirname)
            if not os.path.exists(spark_df_dirname):
                spark_df.write.option("header", True).csv(spark_df_dirname)
            for root, _, files in os.walk(spark_df_dirname):
                for f in files:
                    filepath = os.path.join(root, f)  
                    if not filepath.endswith("v"):
                        os.remove(filepath) 
        return spark_df
###################END_SPARKCLASS###########################################




import hashlib
import datetime

'''WHAT ABOUT BLOCK CHAIN FOR ELECTIONS?'''

class TicketBlock:
    def __init__(self):
        '''the use of blockchain for the purpose of securing election polling results'''
        self.chain = []
        self.construct_blockchain(proof=1, previous_hash='0')
       
    def construct_blockchain(self, proof, previous_hash):
        block = {'index' : len(self.chain) + 1,
                 'timestamp' : str(datetime.datetime.now()),
                 'proof' : proof,
                 'previous_hash' : previous_hash}
        self.chain.append(block)
        return block

    def get_previous_block(self):
        last_block = self.chain[-1]
        return last_block 
    
    def proof_of_work(self, previous_proof):
        new_proof = 1
        check_proof = False 
        while check_proof is False:
            hash_operation = hashlib\
                             .sha256(str(new_proof ** 2 - previous_proof ** 2)\
                             .encode()).hexdigest()
            if hash_operation[:4] == '0000':
                check_proof = True
            else:
                new_proof += 1
        return new_proof 

    def hash(self, block):
        encoded_block = json.dumps(block, sort_keys=True).encode()
        return hashlib.sha256(encoded_block).hexdigest()
###########################END BLOCK CLASS#############################    
    
    
    
    
    
    
'''studentDf = self.spark.createDataFrame([Row(id=1,name='alex',marks=67),
                                            Row(id=2,name='rose',marks=88),
                                            Row(id=3,name='kita',marks=79),
                                            Row(id=4,name='mike',marks=67)])
        studentDf.select("id","name","marks").write.format("jdbc").option("url", "jdbc:postgresql://localhost:5432/testing_")\
            .option("driver", "org.postgresql.Driver").option("dbtable", "students").option("user", "postgres")\
                .option("password", "07141989").save()'''