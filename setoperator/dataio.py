import polars as pl
import pandas as pd
import dask.dataframe as dd
import duckdb


def readdfs(path: str, input_files: list[str], dftype:str) -> list[object]:
    df_list = []
    for file in input_files:
        file_extension = file.split(".")[-1]
        file_path = f'{path}/{file}'
        if dftype == "polars":
            if file_extension == "xlsx":
                df_list.append(pl.read_excel(file_path))
            elif file_extension == "csv":
                df_list.append(pl.read_csv(file_path))
            elif file_extension == "tsv":
                df_list.append(pl.read_csv(file_path, sep="\t"))
            elif file_extension == "jsonl":
                df_list.append(pl.read_ndjson(file_path))
            else:
                raise Exception("지원하지 않는 파일 포맷")
        elif dftype == "dask":
            if file_extension == "xlsx":
                df_list.append(dd.from_pandas(pd.read_excel(file_path), npartitions=8))
            elif file_extension == "csv":
                df_list.append(dd.read_csv(file_path))
            elif file_extension == "tsv":
                df_list.append(dd.read_csv(file_path, sep="\t"))
            elif file_extension == "jsonl":
                df_list.append(dd.read_json(file_path, lines=True))
            else:
                raise Exception("지원하지 않는 파일 포맷")
        elif dftype == "pandas":
            if file_extension == "xlsx":
                df_list.append(pd.read_excel(file_path))
            elif file_extension == "csv":
                df_list.append(pd.read_csv(file_path))
            elif file_extension == "tsv":
                df_list.append(pd.read_csv(file_path, sep="\t"))
            elif file_extension == "jsonl":
                df_list.append(pd.read_json(file_path, lines=True))
            else:
                raise Exception("지원하지 않는 파일 포맷")
        elif dftype == "duckdb":
            if file_extension == "xlsx":
                df_list.append(pd.read_excel(file_path))
            elif file_extension == "csv":
                df_list.append(duckdb.read_csv(file_path))
            elif file_extension == "tsv":
                df_list.append(duckdb.read_csv(file_path, sep="\t"))
            elif file_extension == "jsonl":
                df_list.append(duckdb.read_json(file_path))
            else:
                raise Exception("지원하지 않는 파일 포맷")
    return df_list


def savedf(df: object, output_path: str, dftype: str):
    file_extension = output_path.split(".")[-1]

    if dftype == "polars":
        if file_extension == "xlsx":
            df.write_excel(output_path)
        elif file_extension == "csv":
            df.write_csv(output_path)
        elif file_extension == "tsv":
            df.write_csv(output_path, sep="\t")
        elif file_extension == "jsonl":
            df.write_ndjson(output_path)
        else:
            raise Exception("지원하지 않는 파일 포맷")
    elif dftype == "dask":
        df = df.compute()
        if file_extension == "xlsx":
            df.to_excel(output_path, index=False)
        elif file_extension == "csv":
            df.to_csv(output_path, index=False)
        elif file_extension == "tsv":
            df.to_csv(output_path, sep="\t", index=False)
        elif file_extension == "jsonl":
            df.to_json(output_path, orient="records", lines=True, force_ascii=False)
        else:
            raise Exception("지원하지 않는 파일 포맷")
    elif dftype == "pandas" or dftype == "duckdb":
        if file_extension == "xlsx":
            df.to_excel(output_path, index=False)
        elif file_extension == "csv":
            df.to_csv(output_path, index=False)
        elif file_extension == "tsv":
            df.to_csv(output_path, sep="\t", index=False)
        elif file_extension == "jsonl":
            df.to_json(output_path, orient="records", lines=True, force_ascii=False)
        else:
            raise Exception("지원하지 않는 파일 포맷")
    else:
        raise Exception
