import polars as pl
import pandas as pd
import dask.dataframe as dd


def readdf(input_path:str, dftype:str) -> object:
    file_extension = input_path.split('.')[-1]

    if dftype == 'polars':
        # excel to df
        if file_extension == 'xlsx': return pl.read_excel(input_path)
        # csv to df
        elif file_extension == 'csv': return pl.read_csv(input_path)
        # tsv to df
        elif file_extension == 'tsv': return pl.read_csv(input_path, sep='\t')
        # jsonl to df
        elif file_extension == 'jsonl': return pl.read_ndjson(input_path)
        else: raise Exception("지원하지 않는 파일 포맷")
    elif dftype == 'dask':
        # excel to df
        if file_extension == 'xlsx': return dd.from_pandas(pd.read_excel(input_path), npartitions=1)
        # csv to df
        elif file_extension == 'csv': return dd.read_csv(input_path)
        # tsv to df
        elif file_extension == 'tsv': return dd.read_csv(input_path, sep='\t')
        # jsonl to df
        elif file_extension == 'jsonl': return dd.read_json(input_path, lines=True)
        else: raise Exception("지원하지 않는 파일 포맷")    
    elif dftype == 'pandas':
        # excel to df
        if file_extension == 'xlsx': return pd.read_excel(input_path, dtype="object")
        # csv to df
        elif file_extension == 'csv': return pd.read_csv(input_path, dtype="object")
        # tsv to df
        elif file_extension == 'tsv': return pd.read_csv(input_path, sep='\t', dtype="object")
        # jsonl to df
        elif file_extension == 'jsonl': return pd.read_json(input_path, lines=True, dtype="object")
        else: raise Exception("지원하지 않는 파일 포맷")
    else:
        raise Exception



def savedf(df:object, output_path:str, dftype:str): # 인자로 dftype 추가
    file_extension = output_path.split('.')[-1]

    if dftype == 'polars':
        # df to excel
        if file_extension == 'xlsx': df.write_excel(output_path)
        # df to csv
        elif file_extension == 'csv': df.write_csv(output_path)
        # df to tsv
        elif file_extension == 'tsv': df.write_csv(output_path, sep='\t')
        # df to jsonl
        elif file_extension == 'jsonl': df.write_ndjson(output_path)
        else: raise Exception("지원하지 않는 파일 포맷")
    if dftype == 'dask':
        # df to excel
        if file_extension == 'xlsx': df.compute().to_excel(output_path, index=False)
        # df to csv
        elif file_extension == 'csv': df.to_csv(output_path)
        # df to tsv
        elif file_extension == 'tsv': df.to_csv(output_path, sep='\t')
        # df to jsonl
        elif file_extension == 'jsonl': df.compute().to_json(output_path, orient='records', lines=True, force_ascii=False)
        else: raise Exception("지원하지 않는 파일 포맷")
    elif dftype == 'pandas':
        # df to excel
        if file_extension == 'xlsx': df.to_excel(output_path, index=False)
        # df to csv
        elif file_extension == 'csv': df.to_csv(output_path, index=False)
        # df to tsv
        elif file_extension == 'tsv': df.to_csv(output_path, sep='\t', index=False)
        # df to jsonl
        elif file_extension == 'jsonl': df.to_json(output_path, orient='records', lines=True, force_ascii=False)
        else: raise Exception("지원하지 않는 파일 포맷")
    else:
        raise Exception