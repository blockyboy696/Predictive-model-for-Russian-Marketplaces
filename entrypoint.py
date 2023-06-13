from model.model import predict
from parser.parser import data_cat_pipeline
from conf.conf import logging,settings

logging.info('What should be done?\n 1.Scrape the data\n 2.Predict')
func = int(input())
if func == 1:
    logging.info('enter category link')
    link = input()
    data_cat_pipeline(link)
elif func == 2:
    logging.info('enter category data path')
    data_path = input()
    logging.info('enter predict data path')
    pred_path = input()
    predict(data_path,pred_path)