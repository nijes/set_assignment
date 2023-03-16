from setoperator import getargs, savedf, PandasOperator
from time import time


def main(project_dir:str, input_list:list[str], operation:str, key_columns:list[str], for_any:bool) -> object:
    pdoperator = PandasOperator(project_dir, input_list, key_columns, for_any)
    return pdoperator(operation)


if __name__ == "__main__":
    
    project_dir, input_files, output_file, operation, key_columns, for_any = getargs()
    
    result_df = main( project_dir, input_files, operation, key_columns, for_any)
    savedf(result_df, output_file)

    #print(result_df)
    #print(len(result_df))