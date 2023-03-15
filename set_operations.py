import argparse
import pandas as pd
from setoperator import PandasOperator


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


def main(df_list, operation, key_columns, for_any):

    pdOperator = PandasOperator()

    if operation == 'intersection':
        return pdOperator.intersection(df_list, key_columns=key_columns, for_any=for_any)
    elif operation == 'diff':
        return pdOperator.diff(df_list, key_columns=key_columns, for_any=for_any)
    elif operation == 'union':
        return pdOperator.union(df_list, key_columns=key_columns, for_any=for_any)
    
    
def readFiles(project_dir, input_files):
    return [ pd.read_excel(project_dir + '/' + file) for file in input_files ]


def saveFile(df, output_file):
    df.to_excel(output_file, index=False)



if __name__ == "__main__":
    
    project_dir, input_files, output_file, operation, key_columns, for_any = get_arguments()

    result = main( readFiles(project_dir, input_files), operation, key_columns, for_any)
    saveFile(result, output_file)

    #print(result)
    #print(len(result))