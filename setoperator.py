import pandas as pd


class PandasOperator():
    """
    ex)
    pdoperator = PandasOperator(df_list=[df1, df2, df3], key_columns=[col1, col2], for_any=True)
    pdoperator(operation=intersection)
    > union_dataframe
    """
    def __init__(self, df_list:list, key_columns: list, for_any: bool):
        self.df_list = df_list
        self.key_columns = key_columns
        self.for_any = for_any

    def __call__(self, operation):
        if operation == 'intersection':
            return self._intersection()
        elif operation == 'diff':
            return self._diff()
        elif operation == 'union':
            return self._union()
        else:
            raise SyntaxError
    
    def _intersection(self):
        result_df = self.df_list[0]
        for sub_df in self.df_list[1:]:
            # 하나의 key_column 값이라도 일치
            if self.for_any:
                for key_column in self.key_columns:
                    if key_column == self.key_columns[0]:
                        result_df = result_df[result_df[key_column].isin(set(result_df[key_column])&set(sub_df[key_column]))]
                    else:
                        result_df = pd.concat([result_df, result_df[result_df[key_column].isin(set(result_df[key_column])&set(sub_df[key_column]))]]).drop_duplicates()
            # 모든 key_column 값이 일치
            else:
                for key_column in self.key_columns:
                    result_df = result_df[result_df[key_column].isin(set(result_df[key_column])&set(sub_df[key_column]))]
                    sub_df = sub_df[sub_df[key_column].isin(set(result_df[key_column])&set(sub_df[key_column]))]
        return result_df


    def _diff(self):
        result_df = self.df_list[0]
        for sub_df in self.df_list[1:]:
            # 하나의 key_column이라도 값이 일치하면 제외
            if self.for_any:
                for key_column in self.key_columns:
                    result_df = result_df[result_df[key_column].isin(set(result_df[key_column])-set(sub_df[key_column]))]
                    sub_df = sub_df[sub_df[key_column].isin(set(sub_df[key_column])-set(result_df[key_column]))]
            # key_column 값이 전부 같은 경우 제외
            else:
                for key_column in self.key_columns:
                    if key_column == self.key_columns[0]:
                        result_df = result_df[result_df[key_column].isin(set(result_df[key_column])-set(sub_df[key_column]))]
                    else:
                        result_df = pd.concat([result_df, result_df[result_df[key_column].isin(set(result_df[key_column])-set(sub_df[key_column]))]]).drop_duplicates()
        return result_df        


    def _union(self):
        result_df = self.df_list[0]
        for sub_df in self.df_list[1:]:
            result_df = pd.concat([result_df, sub_df])
            # key_column 값이 하나라도 같으면 중복 제거
            if self.for_any:
                for key_column in self.key_columns:
                    result_df = result_df.drop_duplicates(subset=[key_column])
            # key_column 값이 전부 같은 경우 중복 제거
            else:
                result_df = result_df.drop_duplicates(subset=self.key_columns)                
        return result_df
    

class DaskOperator():
    pass


class PolarsOperator():
    pass