from data.matrix import get_split
from conf.conf import logging,settings
from xgboost import XGBRegressor

def predict(dir1,dir2):
    xgb = XGBRegressor()
    features, pred, target = get_split(dir1,dir2)
    xgb.fit(features,target)
    prediction = xgb.predict(pred)
    logging.info(f'predicted prices are:{prediction}')
