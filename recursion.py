import time
import pandas as pd 

def recursive_action(x, action, **kwargs):
    """
        recursive_function(x, action)
        This function acts on each element of x by action
    """
    try:
        elem = next(x)
        action(elem, **kwargs)
        recursive_action(x, action, **kwargs)
    except StopIteration:
        pass


def check_visits(iter_visits, users_visits, i):
    i += 1
    cols = ['id', 'Reg_date', 'id_partner', 'name', 'visits']
    try:
        chunk = next(iter_visits)
        users_visits = check_visits(iter_visits, users_visits, i)
        t = time.time()
    
        chunk['Visit_date'] = pd.to_datetime(chunk['Visit_date'])
        chunk['Visit_date'] = chunk['Visit_date'].dt.floor('d')
        chunk = chunk.groupby('id_user', as_index=False).min()    
        
        users_visits = pd.merge(users_visits, chunk, how='left', left_on=['id'], right_on=['id_user'])
        users_visits.visits = users_visits.visits + 1 * ((users_visits.Reg_date + pd.DateOffset(6)) >= users_visits.Visit_date)
        users_visits = users_visits[cols]

        print('{}) checked last {} rows \t\t time - {} sec'.format(i, i*1000000,  time.time() - t))
        del chunk
        del t
        del cols
        return users_visits
    except StopIteration:
        return users_visits

def count_visits(iter_visits, users_visits, i):
    i += 1
    cols = ['id', 'id_partner', 'name', 'visits']
    try:
        chunk = next(iter_visits)
        users_visits = count_visits(iter_visits, users_visits, i)
        t = time.time()

        chunk = chunk.assign(n=1)
        chunk = chunk[['id_user', 'n']]
        chunk = chunk.groupby('id_user', as_index=False).sum() 

        users_visits = pd.merge(users_visits, chunk, how='left', left_on=['id'], right_on=['id_user'])
        users_visits.n = users_visits.n.fillna(0)
        users_visits.visits = users_visits.visits + users_visits.n
        users_visits = users_visits[cols]

        print('{}) checked last {} rows \t\t time - {} sec'.format(i, i*1000000,  time.time() - t))
        del chunk
        del t
        del cols
        return users_visits
    except StopIteration:
        return users_visits

def last_visit(iter_visits, users_visits, i):
    i += 1
    cols = ['id', 'Reg_date', 'id_partner', 'name', 'visits']
    try:
        chunk = next(iter_visits)
        users_visits = last_visit(iter_visits, users_visits, i)
        t = time.time()

        chunk['Visit_date'] = pd.to_datetime(chunk['Visit_date'])
        chunk['Visit_date'] = chunk['Visit_date'].dt.floor('d')
        chunk = chunk.groupby('id_user', as_index=False).max()
        
        users_visits = pd.merge(users_visits, chunk, how='left', left_on=['id'], right_on=['id_user'])
        users_visits.Visit_date = users_visits.Visit_date.fillna(users_visits.Reg_date)

        users_visits.visits =  users_visits.visits.where((users_visits.Visit_date - users_visits.Reg_date) < users_visits.visits, (users_visits.Visit_date - users_visits.Reg_date))
        users_visits = users_visits[cols]

        print('{}) checked last {} rows \t\t time - {} sec'.format(i, i*1000000,  time.time() - t))
        del chunk
        del t
        del cols
        return users_visits
    except StopIteration:
        return users_visits

        
