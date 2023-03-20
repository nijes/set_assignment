import pandas as pd
from .dataio import readdf
from functools import reduce


class SetOperator:
    def __init__(self, path:str, input_list:list[str], key_columns:list[str], for_any:bool):
        self.path = path
        self.input_list = input_list
        self.key_columns = key_columns
        self.for_any = for_any


class PandasOperator(SetOperator):
    def __init__(self, path:str, input_list:list[str], key_columns:list[str], for_any:bool):
        super().__init__(path, input_list, key_columns, for_any)

    def __call__(self, operation:str):
        if operation == 'intersection':
            return self._intersection()
        elif operation == 'diff':
            return self._diff()
        elif operation == 'union':
            return self._union()
        else:
            raise SyntaxError


    def _intersection_condition(self, main_df:object, sub_df:object) -> object:

        condition_list = []
        for key_column in self.key_columns:
            condition_list.append(main_df[key_column].isin(set(sub_df[key_column])))
        
        if self.for_any:
            condition = reduce(lambda x, y: x | y, condition_list)
        else:
            condition = reduce(lambda x, y: x & y, condition_list)

        return condition
        


    def _intersection(self) -> object:
        result_df = readdf(input_path=f'{self.path}/{self.input_list[0]}')

        for input in self.input_list[1:]:
            sub_df = readdf(input_path=f'{self.path}/{input}')
            result_df = result_df[self._intersection_condition(result_df, sub_df)]

        return result_df


    def _diff(self) -> object:
        result_df = readdf(input_path=f'{self.path}/{self.input_list[0]}')

        for input in self.input_list[1:]:
            sub_df = readdf(input_path=f'{self.path}/{input}')
            result_df = result_df[~self._intersection_condition(result_df, sub_df)]
        
        return result_df       


    def _union(self) -> object:
        result_df = readdf(input_path=f'{self.path}/{self.input_list[0]}')

        for input in self.input_list[1:]:
            sub_df = readdf(input_path=f'{self.path}/{input}')
            sub_df = sub_df[~self._intersection_condition(sub_df, result_df)][result_df.columns]            
            result_df = pd.concat([result_df, sub_df])

        return result_df