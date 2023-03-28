import argparse


def getargs(): 
    #path[str], output_file[str], input_files[list], operation[str], key_columns[list], for_any[bool], dftype[str]
    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--path", dest="path", required=True)
    parser.add_argument("-o", "--output", dest="output_file", required=True)
    parser.add_argument("-i", "--input", dest="input_files", nargs="+", required=True)

    parser.add_argument("--diff", dest="operation", action="store_const", const="diff")
    parser.add_argument("--intersection", dest="operation", action="store_const", const="intersection")
    parser.add_argument("--union", dest="operation", action="store_const", const="union")
    
    parser.add_argument("--key_columns", dest="key_columns", nargs="+", required=True)
    parser.add_argument("--for_any_key_columns", dest="for_any", action="store_true", default=False)
    
    parser.add_argument("--dftype", dest="dftype", choices=["pandas", "polars", "dask", "duckdb"])

    args = parser.parse_args()

    return args