import pandas as pd
from .dataio import readdf


class SetOperator:
    def __init__(self, path:str, input_list:list[str], key_columns:list[str], for_any:bool):
        self.main_df = readdf(f'{path}/{input_list[0]}')
        self.project_dir = path
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
    
    def _intersection(self):
        result_df = self.main_df
        for input in self.input_list[1:]:
            temp_df = result_df.copy()
            sub_df = readdf(input_path=f'{self.project_dir}/{input}')[self.key_columns]
            # 하나의 key_column 값이라도 일치
            if self.for_any:
                for key_column in self.key_columns: # 컬럼명이 아니라 인덱스로 구분
                    if key_column == self.key_columns[0]: 
                        result_df = result_df[result_df[key_column].isin(set(sub_df[key_column]))]
                    else:
                        result_df = pd.concat([result_df, temp_df[temp_df[key_column].isin(set(sub_df[key_column]))]]).drop_duplicates()
                    # 전체 데이터프레임이 아니라 key column의 값만으로 해당 여부 판단 -> 기준 데이터프레임에서 뽑아내기
            # 모든 key_column 값이 일치
            else:
                set_of_key_columns = set(sub_df[self.key_columns].apply(lambda x: tuple(x), axis=1))
                result_df = result_df[result_df[self.key_columns].apply(lambda x: tuple(x), axis=1).isin(set_of_key_columns)]
        return result_df


    def _diff(self):
        result_df = self.main_df
        for input in self.input_list[1:]:
            sub_df = readdf(input_path=f'{self.project_dir}/{input}')[self.key_columns]
            # key_column 값이 하나라도 일치하면 제외
            if self.for_any:
                for key_column in self.key_columns:
                    result_df = result_df[~result_df[key_column].isin(set(sub_df[key_column]))]
            # key_column 값이 전부 같은 경우 제외
            else:
                set_of_key_columns = set(sub_df[self.key_columns].apply(lambda x: tuple(x), axis=1))
                result_df = result_df[~result_df[self.key_columns].apply(lambda x: tuple(x), axis=1).isin(set_of_key_columns)]
        return result_df        


    def _union(self):
        result_df = self.main_df
        for input in self.input_list[1:]:
            sub_df = readdf(input_path=f'{self.project_dir}/{input}')
            # sub_df 데이터 중 key_column 값이 하나라도 일치하면 제외
            if self.for_any:
                for key_column in self.key_columns:
                    sub_df = sub_df[~sub_df[key_column].isin(set(result_df[key_column]))]      
            # sub_df 데이터 중 key_column 값이 전부 일치하면 제외
            else:
                set_of_key_columns = set(result_df[self.key_columns].apply(lambda x: tuple(x), axis=1))
                sub_df = sub_df[~sub_df[self.key_columns].apply(lambda x: tuple(x), axis=1).isin(set_of_key_columns)]
            result_df = pd.concat([result_df, sub_df])[self.main_df.columns]                                
        return result_df
    


class DaskOperator(SetOperator):
    pass



class PolarsOperator(SetOperator):
    pass