import argparse

# python3 set_operations.py -p ~/project/data -o output.xlsx -i sample_1.xlsx sample_2.xlsx --intersection --key_columns group_id en_id --for_any_key_columns
def get_arguments():

    parser = argparse.ArgumentParser()

    parser.add_argument("-p", "--project", dest="project", action="store", required=True)
    parser.add_argument("-o", "--output", dest="output", action="store", required=True)
    parser.add_argument("-i", "--input", dest="input", nargs="+", action="store", required=True)

    parser.add_argument("--diff", dest="operation", action="store_const", const="diff", required=False)
    parser.add_argument("--intersection", dest="operation", action="store_const", const="intersection", required=False)
    parser.add_argument("--union", dest="operation", action="store_const", const="union", required=False)
    
    parser.add_argument("--key_columns", dest="key_columns", nargs="+", action="store", required=True)
    parser.add_argument("--for_any_key_columns", dest="any", action="store_true", required=False, default=False)
    
    args = parser.parse_args()

    #print("위치:", args.project, "입력:", args.input, "출력:", args.output)
    #print(args.operation)
    #print(args.key_columns)

    return args.project, args.input, args.output, args.operation, args.key_columns, args.any



if __name__ == "__main__":
    project, input, output, operation, key_columns, any = get_arguments()