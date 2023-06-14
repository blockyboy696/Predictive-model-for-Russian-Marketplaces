from conf.conf import logging,settings
from util.util import save_data_csv,get_data_csv
import pandas as pd
import numpy as np
from string import punctuation
from sklearn import preprocessing
from nltk.stem.porter import PorterStemmer
import re


TARGET ='discounted Price'


def get_clean_data(df:pd.DataFrame):
    logging.info('cleaning data')
    try:
        df = df.rename(columns={'barnd':'brand'})
        df = df.drop(['price','discount','id'],axis=1)
        for column in df.columns:
            if df[column].isnull().sum()/len(df) >= 0.15:
             df = df.drop(column,axis=1)
        logging.info('data is cleaned')
        return df
    except:
        for column in df.columns:
            if df[column].isnull().sum()/len(df) >= 0.15:
                df = df.drop(column,axis=1)
        logging.info('data is cleaned')
        return df


def get_column_cat(df):
    print(df.columns)
    CATEGORICAL_COLUMNS = []
    NUMERIC_COLUMNS = []
    for column in df:
        logging.info(f'is {column} numeric,categorical or target column?')
        col_input = input()
        if col_input == 'numeric':
            NUMERIC_COLUMNS.append(column)
        elif col_input == 'categorical':
            CATEGORICAL_COLUMNS.append(column)
    return NUMERIC_COLUMNS,CATEGORICAL_COLUMNS


def preprocess_num_col(NUMERIC_COLUMNS,df):
    logging.info('preprocessing numeric columns')
    for column in NUMERIC_COLUMNS:
        df[column] = df[column].apply(lambda value: re.sub('\D', '', str(value)))
        df[column] = df[column].replace('',0)
        df[column] = df[column].apply(lambda value: int(value))
    return df


def remove_punctuation(sentence: str) -> str:
    return sentence.translate(str.maketrans('', '',punctuation))       

        
def pipeline(data: pd.DataFrame, categorical_features: list, numeric_features: list)-> pd.DataFrame:
    
    porter = PorterStemmer()
    logging.info('preprocessing data')
    
    oe = preprocessing.OrdinalEncoder()
    ss = preprocessing.RobustScaler()
    
    for feature in categorical_features:
        data[feature] = data[feature].replace(np.nan,'')
        data[feature] = data[feature].apply(remove_punctuation)
        data[feature] = data[feature].apply(porter.stem)

    data['Описание'] = data['Описание'].apply(remove_punctuation)
    data['Описание'] = data['Описание'].apply(porter.stem)
        
        
    for feature in numeric_features:
        percentile_90 = np.percentile(data[feature], 75)
        data[feature] = np.clip(data[feature], 0, percentile_90)
        data[feature] = ss.fit_transform(np.array(data[feature]).reshape((-1, 1)))
        data[feature] = data[feature].replace(np.nan,data[feature].mean())
        
    logging.info('preprocessing done')
    return data


def get_df_prep(df):
    logging.info('Enter df name')
    name = input()
    df = get_clean_data(df)
    NUMERIC_COLUMNS,CATEGORICAL_COLUMNS = get_column_cat(df)
    df = preprocess_num_col(NUMERIC_COLUMNS,df)
    df = pipeline(df,CATEGORICAL_COLUMNS,NUMERIC_COLUMNS)
    save_data_csv(f'catalogue/{name}_preprocessed.csv',df)



