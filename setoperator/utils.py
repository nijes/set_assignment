import argparse

def getargs():

    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--path", dest="path", action="store", required=True)
    parser.add_argument("-o", "--output", dest="output_file", action="store", required=True)
    parser.add_argument("-i", "--input", dest="input_files", nargs="+", action="store", required=True)

    parser.add_argument("--diff", dest="operation", action="store_const", const="diff", required=False)
    parser.add_argument("--intersection", dest="operation", action="store_const", const="intersection", required=False)
    parser.add_argument("--union", dest="operation", action="store_const", const="union", required=False)
    
    parser.add_argument("--key_columns", dest="key_columns", nargs="+", action="store", required=True)
    parser.add_argument("--for_any_key_columns", dest="for_any", action="store_true", required=False, default=False)
    
    parser.add_argument("--pandas", dest="framework", action="store_const", const="pandas", required=False, default="pandas")
    parser.add_argument("--polars", dest="framework", action="store_const", const="polars", required=False, default="pandas")
    parser.add_argument("--dask", dest="framework", action="store_const", const="dask", required=False, default="pandas")

    args = parser.parse_args()

    return args.path, args.input_files, args.output_file, args.operation, args.key_columns, args.for_any, args.framework


def add_col_func(df, key_columns):
    for key_column in key_columns:
        if key_column not in df.columns:
            df[key_column] = ''
