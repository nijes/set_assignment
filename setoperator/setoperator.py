import pandas as pd
import polars as pl
import dask.dataframe as dd
import dask.delayed
import duckdb
from .dataio import readdf
from functools import reduce



class SetOperator:
    def __init__(self, path:str, input_list:list[str], key_columns:list[str], for_any:bool):
        self.path = path
        self.input_list = input_list
        self.key_columns = key_columns
        self.for_any = for_any

    def __call__(self, operation:str):
        if operation == 'intersection': return self._intersection()
        elif operation == 'diff': return self._diff()
        elif operation == 'union': return self._union()
        else: raise Exception




class PandasOperator(SetOperator):
    def __init__(self, path:str, input_list:list[str], key_columns:list[str], for_any:bool):
        super().__init__(path, input_list, key_columns, for_any)
        self.dftype = 'pandas'


    def _intersection_condition(self, main_df:object, sub_df:object) -> object:
        condition_list =  [main_df[key_column].isin(set(sub_df[key_column])) for key_column in self.key_columns]
           
        if self.for_any:
            condition = reduce(lambda x, y: x | y, condition_list)
        else:
            condition = reduce(lambda x, y: x & y, condition_list)

        return condition
        
    def _intersection(self) -> object:
        result_df = readdf(f'{self.path}/{self.input_list[0]}', self.dftype)

        for input in self.input_list[1:]:
            sub_df = readdf(f'{self.path}/{input}', self.dftype)
            result_df = result_df[self._intersection_condition(result_df, sub_df)]
        
        return result_df

    def _diff(self) -> object:
        result_df = readdf(f'{self.path}/{self.input_list[0]}', self.dftype)

        for input in self.input_list[1:]:
            sub_df = readdf(f'{self.path}/{input}', self.dftype)
            result_df = result_df[~self._intersection_condition(result_df, sub_df)]

        return result_df       

    def _union(self) -> object:
        result_df = readdf(f'{self.path}/{self.input_list[0]}', self.dftype)
        result_df_cols = result_df.columns

        for input in self.input_list[1:]:
            sub_df = readdf(f'{self.path}/{input}', self.dftype)
            sub_df = sub_df[~self._intersection_condition(sub_df, result_df)]        
            result_df = pd.concat([result_df, sub_df])[result_df_cols]

        return result_df
    



class PolarsOperator(SetOperator):
    def __init__(self, path:str, input_list:list[str], key_columns:list[str], for_any:bool):
        super().__init__(path, input_list, key_columns, for_any)
        self.dftype = 'polars'


    def _intersection_condition(self, sub_df:object) -> object:
        condition_list = []
        for key_column in self.key_columns:
            sub_df_kcol_values = list(map(lambda x: x[key_column], sub_df.select([key_column]).rows(named=True)))
            condition_list.append(pl.col(key_column).is_in(sub_df_kcol_values))
        
        if self.for_any:
            condition = reduce(lambda x, y: x | y, condition_list)
        else:
            condition = reduce(lambda x, y: x & y, condition_list)

        return condition
        
    def _intersection(self) -> object:
        result_df = readdf(f'{self.path}/{self.input_list[0]}', self.dftype)

        for input in self.input_list[1:]:
            sub_df = readdf(f'{self.path}/{input}', self.dftype)
            result_df = result_df.filter(self._intersection_condition(sub_df))

        return result_df

    def _diff(self) -> object:
        result_df = readdf(f'{self.path}/{self.input_list[0]}', self.dftype)

        for input in self.input_list[1:]:
            sub_df = readdf(f'{self.path}/{input}', self.dftype)
            result_df = result_df.filter(~self._intersection_condition(sub_df))
        
        return result_df       

    def _union(self) -> object:
        result_df = readdf(f'{self.path}/{self.input_list[0]}', self.dftype)

        for input in self.input_list[1:]:
            sub_df = readdf(f'{self.path}/{input}', self.dftype)
            sub_df = sub_df.filter(~self._intersection_condition(result_df))        
            result_df = pl.concat([result_df, sub_df], how='diagonal').select(result_df.columns)

        return result_df
    



class DaskOperator(SetOperator):
    def __init__(self, path:str, input_list:list[str], key_columns:list[str], for_any:bool):
        super().__init__(path, input_list, key_columns, for_any)
        self.dftype = 'dask'

    
    def _intersection_condition(self, main_df:object, sub_df:object) -> object:
        condition_list =  [main_df[key_column].isin(set(sub_df[key_column])) for key_column in self.key_columns]

        if self.for_any:
            condition = reduce(lambda x, y: x | y, condition_list)
        else:
            condition = reduce(lambda x, y: x & y, condition_list)

        return condition
    
    @dask.delayed
    def _intersection(self) -> object:
        result_df = readdf(f'{self.path}/{self.input_list[0]}', self.dftype)

        for input in self.input_list[1:]:
            sub_df = readdf(f'{self.path}/{input}', self.dftype)
            result_df = result_df[self._intersection_condition(result_df, sub_df)]

        return result_df.compute()

    @dask.delayed
    def _diff(self) -> object:
        result_df = readdf(f'{self.path}/{self.input_list[0]}', self.dftype)

        for input in self.input_list[1:]:
            sub_df = readdf(f'{self.path}/{input}', self.dftype)
            result_df = result_df[~self._intersection_condition(result_df, sub_df)]
        
        return result_df.compute()       

    @dask.delayed
    def _union(self) -> object:
        result_df = readdf(f'{self.path}/{self.input_list[0]}', self.dftype)

        for input in self.input_list[1:]:
            sub_df = readdf(f'{self.path}/{input}', self.dftype)
            sub_df = sub_df[~self._intersection_condition(sub_df, result_df)]    
            result_df = dd.concat([result_df, sub_df], axis=0)[result_df.columns]

        return result_df.compute()
    



class DuckdbOperator(SetOperator):
    def __init__(self, path:str, input_list:list[str], key_columns:list[str], for_any:bool):
        super().__init__(path, input_list, key_columns, for_any)
        self.dftype = 'duckdb'


    def _duckdb_queryResult(self, query_str:str) -> object:
        result_df = readdf(f'{self.path}/{self.input_list[0]}', self.dftype)

        for input in self.input_list[1:]:
            sub_df = readdf(f'{self.path}/{input}', self.dftype)
            result_df = duckdb.query(query_str).df()

        return result_df

    def _intersection(self) -> object:       
        connecting_condition = 'or' if self.for_any else 'and'
        query_str = f'select * from result_df where' + \
                    f'{connecting_condition}'.join(f' {key_column} in (select {key_column} from sub_df) ' for key_column in self.key_columns)
   
        return self._duckdb_queryResult(query_str)

    def _diff(self) -> object:
        connecting_condition = 'and' if self.for_any else 'or'
        query_str = f'select * from result_df where' + \
                    f'{connecting_condition}'.join(f' {key_column} not in (select {key_column} from sub_df) ' for key_column in self.key_columns) 
               
        return self._duckdb_queryResult(query_str)      

    def _union(self) -> object:
        connecting_condition = 'and' if self.for_any else 'or'
        query_str = f'select * from result_df union all ' + \
                    f'select * from sub_df where' + \
                    f'{connecting_condition}'.join(f' {key_column} not in (select {key_column} from result_df) ' for key_column in self.key_columns)
        
        return self._duckdb_queryResult(query_str)