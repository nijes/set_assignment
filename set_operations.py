from setoperator import getargs, savedf, PandasOperator, PolarsOperator, DaskOperator, DuckdbOperator
from time import time
import dask


def operationResult(path:str, input_list:list[str], operation:str, key_columns:list[str], for_any:bool, dftype:str) -> object:

    if dftype == 'pandas':    
        operator = PandasOperator(path, input_list, key_columns, for_any)
    elif dftype == 'polars':
        operator = PolarsOperator(path, input_list, key_columns, for_any)
    elif dftype == 'dask':
        dask.config.set(scheduler='threads', num_workers=8)
        operator = DaskOperator(path, input_list, key_columns, for_any)
    elif dftype == 'duckdb':
        operator = DuckdbOperator(path, input_list, key_columns, for_any)
        
    result_df = operator(operation)
    return result_df


if __name__ == "__main__":

    start = time()

    args = getargs()
    result_df = operationResult(args.path, args.input_files, args.operation, args.key_columns, args.for_any, args.dftype)
    savedf(result_df, f'{args.path}/{args.output_file}', args.dftype)
    
    print('timer:', time()-start, 'sec')


