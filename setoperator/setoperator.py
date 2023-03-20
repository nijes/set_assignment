import pandas as pd
from .dataio import readdf


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


    def _intersection_filter(self, main_kcol_set:set, sub_kcol_set:set) -> set:
        if self.for_any:
            intersection_filter_set = set()
            for i in range(len(self.key_columns)):
                intersection_filter_set.update(set(filter(lambda x: x[i] in set(map(lambda x: x[i], sub_kcol_set)), main_kcol_set)))
        else:
            intersection_filter_set = main_kcol_set & sub_kcol_set

        return intersection_filter_set
        


    def _intersection(self) -> object:
        main_df = readdf(input_path=f'{self.path}/{self.input_list[0]}')
        intersection_filter_set = set(main_df[self.key_columns].apply(lambda x: tuple(x), axis=1))

        for input in self.input_list[1:]:
            sub_df = readdf(input_path=f'{self.path}/{input}')[self.key_columns]
            sub_kcol_set = set(sub_df.apply(lambda x: tuple(x), axis=1))
            intersection_filter_set = self._intersection_filter(intersection_filter_set, sub_kcol_set)

        result_df = main_df[main_df[self.key_columns].apply(lambda x: tuple(x), axis=1).isin(intersection_filter_set)]
        
        return result_df


    def _diff(self) -> object:
        main_df = readdf(input_path=f'{self.path}/{self.input_list[0]}')
        diff_filter_set = set(main_df[self.key_columns].apply(lambda x: tuple(x), axis=1))

        for input in self.input_list[1:]:
            sub_df = readdf(input_path=f'{self.path}/{input}')[self.key_columns]
            sub_kcol_set = set(sub_df.apply(lambda x: tuple(x), axis=1))
            diff_filter_set = diff_filter_set - self._intersection_filter(diff_filter_set, sub_kcol_set)

        result_df = main_df[main_df[self.key_columns].apply(lambda x: tuple(x), axis=1).isin(diff_filter_set)]
        
        return result_df       


    def _union(self) -> object:
        result_df = readdf(input_path=f'{self.path}/{self.input_list[0]}')

        for input in self.input_list[1:]:
            sub_df = readdf(input_path=f'{self.path}/{input}')
            main_kcol_set = set(result_df[self.key_columns].apply(lambda x: tuple(x), axis=1))
            sub_kcol_set = set(sub_df[self.key_columns].apply(lambda x: tuple(x), axis=1))  
            sub_df = sub_df[~sub_df[self.key_columns].apply(lambda x: tuple(x), axis=1).isin(self._intersection_filter(sub_kcol_set, main_kcol_set))]
            result_df = pd.concat([result_df, sub_df])[self.main_df.columns]

        return result_df
    


class DaskOperator(SetOperator):
    pass



class PolarsOperator(SetOperator):
    pass