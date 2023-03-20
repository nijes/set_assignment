from setoperator import getargs, savedf, PandasOperator, PolarsOperator, DaskOperator
from time import time

def operationResult(path:str, input_list:list[str], operation:str, key_columns:list[str], for_any:bool, dftype:str) -> object:

    if dftype == 'pandas':    
        operator = PandasOperator(path, input_list, key_columns, for_any)
    elif dftype == 'polars':
        operator = PolarsOperator(path, input_list, key_columns, for_any)
    elif dftype == 'dask':
        operator = DaskOperator(path, input_list, key_columns, for_any)

    result_df = operator(operation)

    return result_df


if __name__ == "__main__":
    start = time()
    
    path, input_files, output_file, operation, key_columns, for_any, dftype = getargs()  
    result_df = operationResult(path, input_files, operation, key_columns, for_any, dftype)
    
    output_path = f'{path}/{output_file}'
    savedf(result_df, output_path, dftype)
    
    print(time()-start, 'sec')
    
    #print(len(result_df))
    #print(result_df.head())