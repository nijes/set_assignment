from setoperator import getargs, savedf, PandasOperator


def main(project_dir:str, input_list:list[str], operation:str, key_columns:list[str], for_any:bool) -> object:
    
    pdoperator = PandasOperator(project_dir, input_list, key_columns, for_any)
    result_df = pdoperator(operation)

    return result_df


if __name__ == "__main__":
    
    project_dir, input_files, output_file, operation, key_columns, for_any = getargs()
    
    result_df = main( project_dir, input_files, operation, key_columns, for_any)
    output_path = f'{project_dir}/{output_file}'
    savedf(result_df, output_path)

    #print(result_df)
    #print(len(result_df))