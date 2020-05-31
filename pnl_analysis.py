import numpy as np
import pandas as pd

portfolio_set = pd.read_json('portfolio_set.json')


def read_position(date):
    temp_position = pd.read_csv(date + '.csv')

    return {'symbol': np.array(temp_position['Symbol']), 'quantity': np.array(temp_position['Quantity']),
            'price': np.array(temp_position['MarkPrice'])}


def calculate_pnl_us(start, end):
    portfolio_set = pd.read_json('portfolio_set.json')
    portfolio_set_us = portfolio_set['US-stocks']
    # print(read_position(start))

if __name__ == '__main__':
    calculate_pnl_us('0528', '0529')
