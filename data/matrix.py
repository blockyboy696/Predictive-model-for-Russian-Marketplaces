import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer,TfidfVectorizer
from collections import Counter
from util.util import get_data_csv
from data.preprocessing import get_column_cat
from sklearn.model_selection import train_test_split
from scipy.sparse import csr_matrix, hstack
from data.preprocessing import get_column_cat
from conf.conf import logging,settings

def get_cat_mat(df,CATEGORICAL_COLUMNS):
    logging.info('categorical matrix building started')
    cv = CountVectorizer(min_df=10)
    matrices = []
    for column in CATEGORICAL_COLUMNS:
        try:
            col_t = cv.fit_transform(df[column])
            matrices.append(col_t)
        except:
            cat_mat = cat_mat
    logging.info('categorical matrix building finished')
    cat_mat = hstack(matrices)
    return cat_mat

def get_desc_mat(df):
    logging.info('description matrix building started')
    tv = TfidfVectorizer(max_features=55000, ngram_range=(1, 2))
    desc_mat = tv.fit_transform(df['Описание'])
    logging.info('description matrix building finished')
    return desc_mat


def reducecolumns(Col):
    logging.info('reducing columns')
    n_docs = Counter(Col.nonzero()[1])
    cols_to_keep = [k for k, v in n_docs.items() if v > .0005 * Col.shape[0]]
    return Col[:, cols_to_keep]


def get_full_mat(df,cat_mat,desc_mat,NUMERIC_COLUMNS):
    logging.info('creating full matrix')
    sparse_merge = hstack((reducecolumns(cat_mat), reducecolumns(desc_mat), df[NUMERIC_COLUMNS])).tocsr()
    return sparse_merge

def get_features(mat,len_pred):
    logging.info('spliting matrix for model training')
    features, pred = mat[:mat.shape[0]-len_pred],mat[-len_pred]
    return features,pred


#NUMERIC_COLUMNS, CATEGORICAL_COLUMNS
def get_split(dir,dir2,TARGET = 'dicsounted Price'):
    df = get_data_csv(dir)
    target = df[TARGET]
    df = df.drop(TARGET,axis=1)
    for_pred = get_data_csv(dir2)
    len_pred = len(for_pred)
    full_df = pd.concat([df,for_pred])
    NUMERIC_COLUMNS, CATEGORICAL_COLUMNS = get_column_cat(full_df)
    for column in CATEGORICAL_COLUMNS:
        full_df[column] = [str(x)for x in full_df[column]]
    cat_mat = get_cat_mat(full_df,CATEGORICAL_COLUMNS)
    desc_mat = get_desc_mat(full_df)
    full_mat = get_full_mat(full_df,cat_mat,desc_mat,NUMERIC_COLUMNS)
    features, pred, = get_features(full_mat,len_pred)
    return features, pred, target