from conf.conf import logging,settings
from util.util import save_data_csv,get_data_csv


def get_clean_data(df:pd.DataFrame):
    for column in df.columns:
        if df[column].isnull().sum()/len(df) >= 0.15:
            df = df.drop(column,axis=1)
    return df

def get_column_cat(df):
    CATEGORICAL_COLUMNS = []
    NUMERIC_COLUMNS = []
    TEXT_COLUMNS = []
    for column in df:
        logging.info(f'is {column} numeric,categorical,text or index column?')
        col_input = input()
        if col_input == 'numeric':
            NUMERIC_COLUMNS.append(column)
        elif col_input == 'categorical':
            CATEGORICAL_COLUMNS.append(column)
        elif col_input == 'text':
            TEXT_COLUMNS.append(column)
    return NUMERIC_COLUMNS,CATEGORICAL_COLUMNS,TEXT_COLUMNS


def preprocess_num_col(NUMERIC_COLUMNS,df):
    for column in NUMERIC_COLUMNS:
        df[column] = df[column].apply(lambda value: re.sub('\D', '', str(value)))
        df[column] = df[column].replace('',0)
        df[column] = df[column].apply(lambda value: int(value))
    return df
        
        
def preprocess_txt_col(TEXT_COLUMNS,df):
    for column in TEXT_COLUMNS:
        df[column] = df[column].str.lower()
    return df


