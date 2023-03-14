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
    
    parser.add_argument("--key_columns", dest="key_columns", nargs="+", action="store", required=True)
    parser.add_argument("--for_any_key_columns", dest="any", action="store_true", required=False, default=False)
    
    args = parser.parse_args()

    #print("위치:", args.project, "입력:", args.input, "출력:", args.output)
    #print(args.operation)
    #print(args.key_columns)

    return args.project_dir, args.input_files, args.output_file, args.operation, args.key_columns, args.any


def main(project_dir, input_files, operation, key_columns):

    df1 = pd.read_excel(project_dir + '/' + input_files[0])
    df2 = pd.read_excel(project_dir + '/' + input_files[1])
    
    key_column = key_columns[0]
    #print(key_column)

    # col_to_set
    df1_set = set(df1[key_column])
    df2_set = set(df2[key_column])

    if operation == 'intersection':
        # intersection
        inter_df = df1[df1[key_column].isin(df1_set&df2_set)]
        return inter_df

    elif operation == 'diff':
        # diff
        diff_df1 = df1[df1[key_column].isin(df1_set-df2_set)]
        diff_df2 = df2[df2[key_column].isin(df2_set-df1_set)]
        diff_df = pd.concat([diff_df1, diff_df2])
        return diff_df

    elif operation == 'union':
        # union
        diff_df2 = df2[df2[key_column].isin(df2_set-df1_set)]
        union_df = pd.concat([df1, diff_df2])
        return union_df



if __name__ == "__main__":
    project_dir, input_files, output_file, operation, key_columns, any = get_arguments()
    print( main(project_dir, input_files, operation, key_columns) )