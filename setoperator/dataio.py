import polars as pl
import pandas as pd
import dask.dataframe as dd
import duckdb


def readdf(input_path:str, dftype:str) -> object:
    file_extension = input_path.split('.')[-1]

    if dftype == 'polars':
        if file_extension == 'xlsx': return pl.read_excel(input_path)
        elif file_extension == 'csv': return pl.read_csv(input_path)
        elif file_extension == 'tsv': return pl.read_csv(input_path, sep='\t')
        elif file_extension == 'jsonl': return pl.read_ndjson(input_path)
        else: raise Exception("지원하지 않는 파일 포맷")
    elif dftype == 'dask':
        if file_extension == 'xlsx': return dd.from_pandas(pd.read_excel(input_path), npartitions=8)
        elif file_extension == 'csv': return dd.read_csv(input_path)
        elif file_extension == 'tsv': return dd.read_csv(input_path, sep='\t')
        elif file_extension == 'jsonl': return dd.read_json(input_path, lines=True)
        else: raise Exception("지원하지 않는 파일 포맷")    
    elif dftype == 'pandas':
        if file_extension == 'xlsx': return pd.read_excel(input_path)
        elif file_extension == 'csv': return pd.read_csv(input_path)
        elif file_extension == 'tsv': return pd.read_csv(input_path, sep='\t')
        elif file_extension == 'jsonl': return pd.read_json(input_path, lines=True)
        else: raise Exception("지원하지 않는 파일 포맷")
    elif dftype == 'duckdb':
        if file_extension == 'xlsx': return pd.read_excel(input_path)
        elif file_extension == 'csv' : return duckdb.read_csv(input_path)
        elif file_extension == 'tsv' : return duckdb.read_csv(input_path, sep='\t')
        elif file_extension == 'jsonl' : return duckdb.read_json(input_path)
        else: raise Exception("지원하지 않는 파일 포맷")
    else:
        raise Exception



def savedf(df:object, output_path:str, dftype:str):
    file_extension = output_path.split('.')[-1]

    if dftype == 'polars':
        if file_extension == 'xlsx': df.write_excel(output_path)
        elif file_extension == 'csv': df.write_csv(output_path)
        elif file_extension == 'tsv': df.write_csv(output_path, sep='\t')
        elif file_extension == 'jsonl': df.write_ndjson(output_path)
        else: raise Exception("지원하지 않는 파일 포맷")
    elif dftype == 'dask':
        if file_extension == 'xlsx': df.compute().to_excel(output_path, index=False)
        elif file_extension == 'csv': df.compute().to_csv(output_path, index=False)
        elif file_extension == 'tsv': df.compute().to_csv(output_path, sep='\t', index=False)
        elif file_extension == 'jsonl': df.compute().to_json(output_path, orient='records', lines=True, force_ascii=False)
        else: raise Exception("지원하지 않는 파일 포맷")
    elif dftype == 'pandas' or dftype == 'duckdb':
        if file_extension == 'xlsx': df.to_excel(output_path, index=False)
        elif file_extension == 'csv': df.to_csv(output_path, index=False)
        elif file_extension == 'tsv': df.to_csv(output_path, sep='\t', index=False)
        elif file_extension == 'jsonl': df.to_json(output_path, orient='records', lines=True, force_ascii=False)
        else: raise Exception("지원하지 않는 파일 포맷")
    else:
        raise Exception