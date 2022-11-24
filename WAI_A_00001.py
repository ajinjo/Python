import dask.dataframe as dd
import pandas as pd
import numpy as np
import pyarrow.parquet as pq
from time import time
import os

def read_dataframe(file_name: str=None, file_type: str=None, use_dask=False, split_row_groups=False, blocksize='default'):
    if file_name is None:
        raise Exception('파일 이름이 없습니다.')

    if file_type is None:
        if len(file_name)>8 and file_name[-8:].lower() == '.parquet':
            file_type = 'parquet'
        elif len(file_name)>4 and file_name[-4:].lower() == '.csv':
            file_type = 'csv'
        else:
            raise Exception('file_type을 선택해주세요.')
            
    elif file_type.lower() == 'csv':
        file_type = 'csv'
    elif file_type.lower() == 'parquet':
        file_type = 'parquet'
    else:
        raise Exception('file_type은 csv 또는 parquet로 지정해주세요.')        

    if os.path.exists(file_name) == False:
        raise Exception('파일이 존재하지 않습니다.')

    if use_dask:
        if file_type == 'csv':
            return dd.read_csv(file_name, blocksize=blocksize)
        else:
            return dd.read_parquet(file_name, split_row_groups=split_row_groups)
    else:
        if file_type == 'csv':
            return pd.read_csv(file_name)
        else:
            return pd.read_parquet(file_name)

def WAI_A_00001(df, method): #method : 결측치 처리 방법 
    
    df = df.apply(pd.to_numeric, errors = 'coerce') #문자열을 숫자로 치환, 숫자로 변경할 수 없는 데이터는 NaN값으로 반환 
    df_dtype = df.apply(lambda x: x.dtype) #dataframe의 dtype
    
    if method is 'pad': 
        return df.fillna(method='pad') #앞선 행의 값으로 NaN값 처리   
    elif method is 'ffill':
        return df.fillna(method='ffill') #앞선 행의 값으로 NaN값 처리
    elif method is 'bfill':
        return df.fillna(method='bfill') #뒤의 행의 값으로 NaN값 처리
    elif method is 'backfill':
        return df.fillna(method='backfill') #뒤의 행의 값으로 NaN값 처리    
    elif method is '0':
        return df.fillna(0) #NaN값 '0'으로 처리
    elif method is '1':
        return df.fillna(1) #NaN값 '1'로 처리
    elif method is 'mean':
        return df.fillna(df.mean()) #NaN값 column의 평균값으로 처리
    elif method is 'linear':
        return df.interpolate(method='linear') #시계열데이터의 값에 선형으로 비례하는 방식 
    elif method is 'time':
        return df.interpolate(method='time') #시계열데이터의 날짜 기준     
    else:
        return df.fillna(method) #그 외 원하는 특정값으로 NaN값 처리 
    