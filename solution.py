import argparse
import logging
from getEngine import GetEngine
from getData import GetBigData
import warnings
import time

def validate_inputs(opt):
    if not isinstance(opt.type, str):
        raise ValueError("Incorrect data format, type should be string")
    return opt

def get_inputs():
    parser = argparse.ArgumentParser()
    parser.add_argument('--type', dest='type', type=str, default='batch')
    args = parser.parse_args()
    print("type: " + args.type)
    return validate_inputs(args)

def main():
    start_time = time.time()
    logging.getLogger().setLevel(logging.INFO)
    warnings.filterwarnings("ignore")
    args = get_inputs()
    
    if args.type == "batch":
        logging.info("batch")
        query = ["SELECT * " +
                "FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2014` "+
                 "where FORMAT_DATETIME('%Y-%m-%d', pickup_datetime) >= '2014-03-01' " +
                 "and FORMAT_DATETIME('%Y-%m-%d', pickup_datetime) <= '2014-03-07' limit 5" ]
    elif args.type == "realtime":
        logging.info("realtime")
        query = ["SELECT * " +
                "FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2014` "+
                "where FORMAT_DATETIME('%Y-%m-%d', pickup_datetime) >= '2014-12-31'" ]
    else:
        raise ValueError("Incorrect type option")
    engine = GetEngine()    
    df = GetBigData.get_df(engine, query)
    database = 'taxi_data'    
    df.to_sql(name='table_taxi', con=GetEngine(database).engine, if_exists = 'append', index=False)
    logging.info("--- Total time : %s seconds ---" % (time.time() - start_time))
    
if __name__ == '__main__':
    main()