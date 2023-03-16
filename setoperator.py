import pandas as pd
import argparse


def getargs():

    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--project", dest="project_dir", action="store", required=True)
    parser.add_argument("-o", "--output", dest="output_file", action="store", required=True)
    parser.add_argument("-i", "--input", dest="input_files", nargs="+", action="store", required=True)

    parser.add_argument("--diff", dest="operation", action="store_const", const="diff", required=False)
    parser.add_argument("--intersection", dest="operation", action="store_const", const="intersection", required=False)
    parser.add_argument("--union", dest="operation", action="store_const", const="union", required=False)
    # operation 1가지만 입력받도록 예외처리할 것
    
    parser.add_argument("--key_columns", dest="key_columns", nargs="+", action="store", required=True)
    parser.add_argument("--for_any_key_columns", dest="for_any", action="store_true", required=False, default=False)
    
    args = parser.parse_args()

    return args.project_dir, args.input_files, args.output_file, args.operation, args.key_columns, args.for_any



def readdf(input_path:str) -> object:    # 인자로 dftype 추가
        file_extension = input_path.split('.')[-1]
        # excel to df
        if file_extension == 'xlsx':
            return pd.read_excel(input_path, dtype="object")
        # csv to df
        elif file_extension == 'csv':
            return pd.read_csv(input_path, dtype="object")
        # tsv to df
        elif file_extension == 'tsv':
            return pd.read_csv(input_path, sep='\t', dtype="object")
        # jsonl to df
        elif file_extension == 'jsonl':
            return pd.read_json(input_path, lines=True, dtype="object")
        else:
            raise Exception("지원하지 않는 파일 포맷")



def savedf(df:object, output_file:str):
    file_extension = output_file.split('.')[-1]
    if file_extension == 'xlsx':
        df.to_excel(output_file, index=False)
    elif file_extension == 'csv':
        df.to_csv(output_file, index=False)
    elif file_extension == 'tsv':
        df.to_csv(output_file, sep='\t', index=False)
    elif file_extension == 'jsonl':
        df.to_json(output_file, orient='records', lines=True)
    else:
        raise Exception("지원하지 않는 파일 포맷")



class PandasOperator():
    """
    ex)
    pdoperator = PandasOperator(df_list=[df1, df2, df3], key_columns=[col1, col2], for_any=True)
    pdoperator(operation=intersection)
    > union_dataframe
    """
    def __init__(self, project_dir:str, input_list:list[str], key_columns:list[str], for_any:bool):
        self.main_df = readdf(f'{project_dir}/{input_list[0]}')
        self.project_dir = project_dir
        self.input_list = input_list
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
        result_df = self.main_df
        for input in self.input_list[1:]:
            temp_df = result_df.copy()
            sub_df = readdf(input_path=f'{self.project_dir}/{input}')[self.key_columns]
            # 하나의 key_column 값이라도 일치
            if self.for_any:
                for key_column in self.key_columns:
                    # if key_column == self.key_columns[0]:
                    #     result_df = result_df[result_df[key_column].isin(set(result_df[key_column])&set(sub_df[key_column]))]
                    # else:
                    #     result_df = pd.concat([result_df, temp_df[temp_df[key_column].isin(set(sub_df[key_column])&set(temp_df[key_column]))]]).drop_duplicates()
                    if key_column == self.key_columns[0]:
                        result_df = result_df[result_df[key_column].isin(set(sub_df[key_column]))]
                    else:
                        result_df = pd.concat([result_df, temp_df[temp_df[key_column].isin(set(sub_df[key_column]))]]).drop_duplicates()
            # 모든 key_column 값이 일치
            else:
                #result_df = pd.merge(result_df, sub_df, on=self.key_columns, how='inner')
                set_of_key_columns = set(sub_df[self.key_columns].apply(lambda x: tuple(x), axis=1))
                result_df = result_df[result_df[self.key_columns].apply(lambda x: tuple(x), axis=1).isin(set_of_key_columns)]
        return result_df


    def _diff(self):
        result_df = self.main_df
        for input in self.input_list[1:]:
            temp_df = result_df.copy()
            sub_df = readdf(input_path=f'{self.project_dir}/{input}')[self.key_columns]
            # key_column 값이 하나라도 일치하면 제외
            if self.for_any:
                for key_column in self.key_columns:
                    result_df = result_df[~result_df[key_column].isin(set(sub_df[key_column]))]
                    #sub_df = sub_df[~sub_df[key_column].isin(set(result_df[key_column]))]
            # key_column 값이 전부 같은 경우 제외
            else:
                # result_df = pd.merge(result_df, sub_df, on=self.key_columns, how='left', indicator=True)
                # result_df = result_df[result_df['_merge']=='left_only'].drop(columns='_merge')
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
                    result_df = pd.concat([result_df, sub_df])        
            # sub_df 데이터 중 key_column 값이 전부 일치하면 제외
            else:
                set_of_key_columns = set(result_df[self.key_columns].apply(lambda x: tuple(x), axis=1))
                sub_df = sub_df[~sub_df[self.key_columns].apply(lambda x: tuple(x), axis=1).isin(set_of_key_columns)]
                result_df = pd.concat([result_df, sub_df])                                
        return result_df[self.main_df.columns]
    

class DaskOperator():
    pass


class PolarsOperator():
    pass