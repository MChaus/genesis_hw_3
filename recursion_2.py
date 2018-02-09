import time
import pandas as pd
import numpy as np


def last_week_visits(iter_visits, current_date):
    try:
        chunk = next(iter_visits)
        lw_visits = last_week_visits(iter_visits, current_date)
        
        chunk['Visit_date'] = pd.to_datetime(chunk['Visit_date'])
        chunk['Visit_date'] = chunk['Visit_date'].dt.floor('d')

        chunk = chunk[(chunk.Visit_date >= current_date - pd.DateOffset(7)) & (chunk.Visit_date < current_date)]
        chunk = chunk.assign(lw_visits=1)
        chunk = chunk[['id_user', 'lw_visits']]
        chunk = chunk.groupby('id_user', as_index=False).sum()

        lw_visits = pd.concat([lw_visits, chunk])

        del chunk
        return lw_visits
    except StopIteration:
        return pd.DataFrame({'id_user':[], 'visits':[]})
        
                
def number_of_letters(current_date, finish_date, Orders, Users, visits_path):
    
    week = current_date.week
    current_week_str = 'week_' + str(week)
    
    if current_date <= finish_date:
        week_letters = number_of_letters((current_date + pd.DateOffset(1)), finish_date, Orders, Users, visits_path)
        t = time.time()
        if ((current_date + pd.DateOffset(1)).week != week) or (current_date == finish_date):
            week_letters = week_letters.assign(week=0)
            week_letters = week_letters.rename(index=str, columns={'week':current_week_str})

        Users_letters = Users[Users.Reg_date < current_date]
        
        iter_visits = pd.read_csv(visits_path, iterator=True, chunksize=1000000)
        lw_visits = last_week_visits(iter_visits, current_date)
        lw_visits.groupby('id_user', as_index=False).sum()
        lw_visits = lw_visits.assign(letters_1 = lambda x: 4 * (x.lw_visits > 7) + 1 * (x.lw_visits <= 7))
        lw_visits = lw_visits[['id_user', 'letters_1']]
        
        Users_letters = pd.merge(Users_letters, lw_visits, how='left', left_on='id', right_on='id_user')  
        del lw_visits
        del Users_letters['id_user']
        Users_letters = Users_letters.fillna(1)
        
        current_orders = Orders[Orders['Order Date'] < current_date]
        current_orders = current_orders[['id_user', 'Amount']].groupby('id_user', as_index = False).sum()
        current_orders = current_orders.assign(letters_2=lambda x: 0 + 6 * (x.Amount >= 1000) + 3 * (x.Amount >= 5000))
        current_orders = current_orders[['id_user', 'letters_2']]
        
        Users_letters = pd.merge(Users_letters, current_orders, how='left', left_on='id', right_on='id_user')
        del current_orders
        del Users_letters['id_user']
        Users_letters = Users_letters.fillna(0)
                                        
        first_order = Orders[Orders['Order Date'] < current_date]
        first_order = first_order[['id_user', 'Order Date']].groupby('id_user', as_index=False).min()
        Users_letters = pd.merge(Users_letters, first_order, how = 'left', left_on='id', right_on='id_user')
        del first_order
        del Users_letters['id_user']
        Users_letters['Order Date'] =  Users_letters['Order Date'].fillna(current_date)
        Users_letters = Users_letters.assign(delay=lambda x: x['Order Date'] - x.Reg_date)
        Users_letters.delay =(Users_letters.delay/ np.timedelta64(1, 'D')).astype(int)
        Users_letters = Users_letters.assign(letters_3=lambda x: 3 + 1 * (x.delay >= 3) + 1 * (x.delay >= 6) )
        del Users_letters['delay']
        del Users_letters['Order Date']

        Users_letters['max_letters'] = Users_letters[['letters_1', 'letters_2', 'letters_3']].max(axis=1)
        del Users_letters['letters_1']
        del Users_letters['letters_2']
        del Users_letters['letters_3']

        week_letters = pd.merge(week_letters, Users_letters, how='left', on=['id', 'Reg_date', 'id_partner' , 'name'])
        week_letters = week_letters.fillna(0)
        week_letters[current_week_str] += week_letters.max_letters
        del week_letters['max_letters']
        del Users_letters

        print('day {},  prcess time {}'.format(current_date, time.time() - t))
        return week_letters
    else:
        return Users[Users.Reg_date < finish_date]
    
    
