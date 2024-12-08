import pandas as pd
import logging

from load_data import load_data, save_data
from config import *

logger = logging.getLogger(__name__)
logging.basicConfig(filename='prep_data.log', level=logging.INFO)


def make_main_graph_data(business_entity_data,
                         business_statistics_data):
    def prep_business_entity_data(data):
        data['formationmy'] = data['entityformdate'].dt.year.astype(str) + '-' + data['entityformdate'].dt.month.astype(str)
        data['formationmy'] = pd.to_datetime(data['formationmy'])
        formations_by_month = data[['formationmy', 'count_entityid']].groupby('formationmy').sum()
        return formations_by_month
    def prep_business_statistics_data(data):
        if data['Period'].dtype != 'datetime64[ns]':
            data['Period'] = pd.to_datetime(data['Period'])
        if data['Value'].dtype != 'int':
            data['Value'] = data['Value'].astype(int)
        data = data.set_index('Period')
        return data
    
    business_entity_data = prep_business_entity_data(business_entity_data)
    business_statistics_data = prep_business_statistics_data(business_statistics_data)
    graph_data = business_statistics_data.merge(business_entity_data,
                                                left_index=True, right_index=True)
    graph_data.columns = ['Business Statistics', 'Business Entities']
    graph_data = graph_data.reset_index().melt(id_vars=['index'], 
                                               value_vars=['Business Statistics', 'Business Entities'])
    graph_data.columns = ['month', 'series', 'value']
    return graph_data


if __name__ == '__main__':
    logger.info('Loading Data')
    business_entity_data = load_data(BUSINESS_ENTITY_PARQUET_PATH)
    business_statistics_data = load_data(BUSINESS_STATISTICS_PARQUET_PATH)

    logger.info('Function: make_main_graph_data')
    main_graph_data = make_main_graph_data(business_entity_data, business_statistics_data)
    save_data(main_graph_data, 'data/main_graph_data.parquet')
