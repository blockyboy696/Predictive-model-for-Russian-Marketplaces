from conf.conf import logging,settings
import pandas as pd

def save_data_csv(dir:str,df:pd.DataFrame) -> None:
    """ 
    Saving table to csv
    """
    logging.info('Saving DataFrame')
    df.to_csv(dir)
    logging.info('DataFrame is saved')
    
    return df

def get_data_csv(link:str) -> pd.DataFrame:
    """ 
    Getting table from csv 
    """
    logging.info('Extracting DataFrame')
    df = pd.read_csv(link)
    logging.info('DataFrame is extracted')
    
    return df