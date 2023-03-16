import argparse
import pandas as pd
from setoperator import PandasOperator
import json


def get_arguments():

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


def main(df_list:list[object], operation:str, key_columns:list[str], for_any:bool) -> object:
    pdoperator = PandasOperator(df_list, key_columns, for_any)
    return pdoperator(operation)


def readFiles(project_dir:str, input_files:list[str]) -> list[object]:    
    df_list = []

    for file in input_files:
        input_path = project_dir + '/' + file
        file_extension = file.split('.')[-1]
        # excel to df
        if file_extension == 'xlsx':
            df_list.append(pd.read_excel(input_path))
        # csv to df
        elif file_extension == 'csv':
            df_list.append(pd.read_csv(input_path))
        # tsv to df
        elif file_extension == 'tsv':
            df_list.append(pd.read_csv(input_path, sep='\t'))
        # jsonl to df
        elif file_extension == 'jsonl':
            df_list.append(pd.read_json(input_path, lines=True))
        else:
            raise Exception("지원하지 않는 파일 포맷")
    return df_list


def saveFile(df:object, output_file:str):
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


if __name__ == "__main__":
    
    project_dir, input_files, output_file, operation, key_columns, for_any = get_arguments()

    result = main( readFiles(project_dir, input_files), operation, key_columns, for_any)
    saveFile(result, output_file)

    #print(result)
    print(len(result))