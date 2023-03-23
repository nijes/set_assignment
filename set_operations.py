from setoperator import getargs, savedf, PandasOperator, PolarsOperator, DaskOperator
from setoperator import DuckdbOperator
from time import time
#from dask.distributed import Client, progress, LocalCluster
import dask

def operationResult(path:str, input_list:list[str], operation:str, key_columns:list[str], for_any:bool, dftype:str) -> object:

    if dftype == 'pandas':    
        operator = PandasOperator(path, input_list, key_columns, for_any)
    elif dftype == 'polars':
        operator = PolarsOperator(path, input_list, key_columns, for_any)
    elif dftype == 'dask':
        #client = Client(n_workers=8, threads_per_worker=2, memory_limit = '16GB')
        dask.config.set(scheduler='threads', num_workers=8)
        operator = DaskOperator(path, input_list, key_columns, for_any)
    elif dftype == 'duckdb':
        operator = DuckdbOperator(path, input_list, key_columns, for_any)
        
    result_df = operator(operation)

    return result_df


if __name__ == "__main__":
    start = time()

    path, input_files, output_file, operation, key_columns, for_any, dftype = getargs()
    result_df = operationResult(path, input_files, operation, key_columns, for_any, dftype)

    output_path = f'{path}/{output_file}'
    
    savedf(result_df, output_path, dftype)
    print(time()-start, 'sec')

