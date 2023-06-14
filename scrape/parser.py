import json
from tqdm import tqdm
import pandas as pd
import requests
from conf.conf import logging,settings
from data.preprocessing import get_df_prep
from util.util import save_data_csv
import math

def get_catalogue(CATALOGUE_PATH=settings.PATH.CATALOGUE_PATH)->list:
    '''
    Returns a catalogue of wb categories and their atributes
    '''
    logging.info('sending request')
    url = 'https://www.wildberries.ru/webapi/menu/main-menu-ru-ru.json'
    headers = {'Accept': "*/*", 'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    response = requests.get(url, headers=headers)
    data = response.json()
    
    logging.info('saving catalogue to json')
    with open(CATALOGUE_PATH, 'w', encoding='UTF-8') as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
    data_list = []
    for d in data:
        try:
            for child in d['childs']:
                try:
                    category_name = child['name']
                    category_url = child['url']
                    shard = child['shard']
                    query = child['query']
                    data_list.append({
                        'category_name': category_name,
                        'category_url': category_url,
                        'shard': shard,
                        'query': query})
                except:
                    continue
                try:
                    for sub_child in child['childs']:
                        category_name = sub_child['name']
                        category_url = sub_child['url']
                        shard = sub_child['shard']
                        query = sub_child['query']
                        data_list.append({
                            'category_name': category_name,
                            'category_url': category_url,
                            'shard': shard,
                            'query': query})
                except:
                    continue
        except:
            continue
    logging.info('catalogue is ready')
    return data_list

def get_cat_params(url, catalog_list):
    """
    Searches by category url and returns its parameters
    """
    try:
        for catalog in catalog_list:
            if catalog['category_url'] == url.split('https://www.wildberries.ru')[-1]:
                logging.info(f'Category found: {catalog["category_name"]}')
                name_category = catalog['category_name']
                shard = catalog['shard']
                query = catalog['query']
                return name_category, shard, query
            else:
                pass
    except:
       logging.info('something went wrong, try again')


def json_to_list(json_file):
    """
    Creates Product Data Frame from json file
    """
    data_list = []
    for data in json_file['data']['products']:
        try:
            price = int(data["priceU"] / 100)
        except:
            price = 0
        data_list.append({
            'name': data['name'],
            'id': data['id'],
            #'discount': data['sale'],
            #'price': price,
            'dicsounted Price': int(data["salePriceU"] / 100),
            'barnd': data['brand']
            
        })
    return data_list


def get_prods(shard, query, low_price=None, top_price=None, ):
    '''
    Scrape product data
    '''
    logging.info('parsing started')
    headers = {'Accept': "*/*", 'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    data_list = []
    for page in tqdm(range(1, 101)):
        url = f'https://catalog.wb.ru/catalog/{shard}/catalog?appType=1&curr=rub&dest=-1075831,-77677,-398551,12358499' \
              f'&locale=ru&page={page}&priceU={low_price * 100};{top_price * 100}' \
              f'&reg=0&regions=64,83,4,38,80,33,70,82,86,30,69,1,48,22,66,31,40&sort=popular&spp=0&{query}'
        r = requests.get(url, headers=headers)
        data = r.json()
        if len(json_to_list(data)) > 0:
            data_list.extend(json_to_list(data))
        else:
            break
    logging.info('parsing started')
    data_list = pd.DataFrame(data_list)
    return data_list

def parser(url, low_price, top_price):
    '''
    Builds df with category
    '''
    catalog_list = get_catalogue()
    try:
        name_category, shard, query = get_cat_params(url=url, catalog_list=catalog_list)
        data_list = get_prods(shard=shard, query=query, low_price=low_price, top_price=top_price)
        
        
        return data_list,name_category
    
    except TypeError:
        logging.info('Error Wrong URL')
    except PermissionError:
        logging.info('Error! Try Again')
        
        
def get_features(id_list):
    '''
    Gets features for the products scrapped before
    '''
    z = pd.DataFrame()
    for id in tqdm(id_list):
        try:
            try:
                shard =  math.floor(id/14000000)
                str_id  = str(id)
                vol = str_id[:-5]
                part = str_id[:-3]
                if shard <10:
                    link = f'https://basket-0{shard}.wb.ru/vol{vol}/part{part}/{str_id}/info/ru/card.json'
                else:
                    link = f'https://basket-{shard}.wb.ru/vol{vol}/part{part}/{str_id}/info/ru/card.json'
                r = requests.get(link)
                r = r.json()
                opt = pd.json_normalize(r['options'])
                opt = opt.set_index('name')
                opt= pd.DataFrame(opt.T)
                opt['id'] = int(id)
                opt['Описание'] = (r['description'])
                z = pd.concat([z,opt])
            except:
                shard =  round(id/14000000)
                str_id  = str(id)
                vol = str_id[:-5]
                part = str_id[:-3]
                if shard <10:
                    link = f'https://basket-0{shard}.wb.ru/vol{vol}/part{part}/{str_id}/info/ru/card.json'
                else:
                    link = f'https://basket-{shard}.wb.ru/vol{vol}/part{part}/{str_id}/info/ru/card.json'
                r = r.json()
                opt = pd.json_normalize(r['options'])
                opt = opt.set_index('name')
                opt= pd.DataFrame(opt.T)
                opt['id'] = int(id)
                opt['Описание'] = (r['description'])
                z = pd.concat([z,opt])
        except:
            z = z
    return z


def data_cat_pipeline(url):
    logging.info('scraping started')
    cat_data,name_category = parser(url,0,1000000000)
    cat_data = pd.DataFrame(cat_data)
    id_list = cat_data['id'].values
    product_data = get_features(id_list)
    data = product_data.merge(cat_data, on='id')
    data = data.set_index('id')
    save_data_csv(f'catalogue/{name_category}.csv')
    get_df_prep(data)
    return data

