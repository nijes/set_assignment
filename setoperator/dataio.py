import polars as pl
import pandas as pd


def readdf(input_path:str) -> object: # 인자로 dftype 추가
        file_extension = input_path.split('.')[-1]
        # excel to df
        if file_extension == 'xlsx':
            return pl.read_excel(input_path)
        # csv to df
        elif file_extension == 'csv':
            return pl.read_csv(input_path)
        # tsv to df
        elif file_extension == 'tsv':
            return pl.read_csv(input_path, sep='\t')
        # jsonl to df
        elif file_extension == 'jsonl':
            return pl.read_ndjson(input_path)
        else:
            raise Exception("지원하지 않는 파일 포맷")
        # # excel to df
        # if file_extension == 'xlsx':
        #     return pd.read_excel(input_path, dtype="object")
        # # csv to df
        # elif file_extension == 'csv':
        #     return pd.read_csv(input_path, dtype="object")
        # # tsv to df
        # elif file_extension == 'tsv':
        #     return pd.read_csv(input_path, sep='\t', dtype="object")
        # # jsonl to df
        # elif file_extension == 'jsonl':
        #     return pd.read_json(input_path, lines=True, dtype="object")
        # else:
        #     raise Exception("지원하지 않는 파일 포맷")

def savedf(df:object, output_path:str): # 인자로 dftype 추가
    file_extension = output_path.split('.')[-1]
    # df to excel
    if file_extension == 'xlsx':
        df.to_excel(output_path, index=False)
    # df to csv
    elif file_extension == 'csv':
        df.to_csv(output_path, index=False)
    # df to tsv
    elif file_extension == 'tsv':
        df.to_csv(output_path, sep='\t', index=False)
    # df to jsonl
    elif file_extension == 'jsonl':
        df.to_json(output_path, orient='records', lines=True, force_ascii=False)
    else:
        raise Exception("지원하지 않는 파일 포맷")