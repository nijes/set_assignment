import argparse
import pandas as pd


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



def main(project_dir, input_files, operation, key_columns, for_any):

    df1 = pd.read_excel(project_dir + '/' + input_files[0])
    df2 = pd.read_excel(project_dir + '/' + input_files[1])


    #operation = 'union'
    #for_any = True

    if operation == 'intersection':
        if for_any:
            for key_column in key_columns:
                df1_set = set(df1[key_column])
                df2_set = set(df2[key_column])
                if key_column == key_columns[0]:
                    inter_df = df1[df1[key_column].isin(df1_set&df2_set)]
                else:
                    inter_df = pd.concat([inter_df, df1[df1[key_column].isin(df1_set&df2_set)]], ignore_index=True).drop_duplicates()
        else:
            for key_column in key_columns:
                df1_set = set(df1[key_column])
                df2_set = set(df2[key_column])
                df1 = df1[df1[key_column].isin(df1_set&df2_set)]
                df2 = df2[df2[key_column].isin(df1_set&df2_set)]
            inter_df = df1
        return inter_df
        

    elif operation == 'diff': #다시해야함
        if for_any:
            for key_column in key_columns:
                df1_set = set(df1[key_column])
                df2_set = set(df2[key_column])
                if key_column == key_columns[0]:
                    diff_df = df1[df1[key_column].isin(df1_set-df2_set)]
                else:
                    diff_df = pd.concat([diff_df, df1[df1[key_column].isin(df1_set-df2_set)]]).drop_duplicates()
        else:
            for key_column in key_columns:
                df1_set = set(df1[key_column])
                df2_set = set(df2[key_column])
                df1 = df1[df1[key_column].isin(df1_set-df2_set)]
                df2 = df2[df2[key_column].isin(df2_set-df1_set)]
            diff_df = df1
        return diff_df
                

    elif operation == 'union':
        union_df = pd.concat([df1, df2])
        if for_any:
            union_df = union_df.drop_duplicates(subset=key_columns)
        else:
            for key_column in key_columns:
                union_df = union_df.drop_duplicates(subset=[key_column])
        return union_df



if __name__ == "__main__":
    project_dir, input_files, output_file, operation, key_columns, for_any = get_arguments()
    #main(project_dir, input_files, operation, key_columns, for_any).to_excel(output_file, index=False)
    print( main(project_dir, input_files, operation, key_columns, for_any) )