from setoperator import getargs, readdfs, savedf
from setoperator import PandasOperator, PolarsOperator, DaskOperator, DuckdbOperator
import dask


def setoperation(
    df_list: list[object],
    operation: str,
    key_columns: list[str],
    for_any: bool,
    dftype: str,
) -> object:
    if dftype == "pandas":
        operator = PandasOperator(df_list, key_columns, for_any)
    elif dftype == "polars":
        operator = PolarsOperator(df_list, key_columns, for_any)
    elif dftype == "dask":
        dask.config.set(scheduler="threads", num_workers=8)
        operator = DaskOperator(df_list, key_columns, for_any)
    elif dftype == "duckdb":
        operator = DuckdbOperator(df_list, key_columns, for_any)
    result_df = operator(operation)
    return result_df


if __name__ == "__main__":
    args = getargs()
    result_df = setoperation(
        df_list=readdfs(args.path, args.input_files, args.dftype),
        operation=args.operation,
        key_columns=args.key_columns,
        for_any=args.for_any,
        dftype=args.dftype,
    )
    savedf(result_df, f"{args.path}/{args.output_file}", args.dftype)
