from File_Handler import File_Handler
from Stats import Stats
from functools import reduce

class Dataframe:
    def __init__(self,data:dict,dtype:dict):
        self.data = data
        self.dtype = dtype
    
    @classmethod
    def read_csv(self, data_path: str, dtype_path: str):
        """
        Reads a CSV file and its corresponding dtype file, and initializes the Dataframe object with the data and dtype.

        Args:
            data_path (str): Path to the CSV data file.
            dtype_path (str): Path to the CSV dtype file.
        
        Return:
            None
        """
        dtype = File_Handler.read_dtype(dtype_path)
        data = File_Handler.read_csv_file(data_path,dtype)

        return Dataframe(data = data, dtype=dtype)
    
    #TODO: define count_nulls()
    def count_nulls(self):
        dict_nulls={}
        for col,val in self.data.items():

            dict_nulls[col] = reduce(lambda cnt,y: cnt+ (1 if y is None else 0),val,0)
        
        return dict_nulls
    
    #TODO: define describe()
    def describe(self) -> None:
        describe_dict = {}

        for col, data_type in self.dtype.items():
            stats = {}

            stats["nulls"] = Stats.get_col_nulls(self.data[col])

            if data_type in ("int", "float"):
                stats["min"] = round(Stats.get_col_min(col = self.data[col]),2)
                stats["max"] = round(Stats.get_col_max(col =self.data[col]),2)
                stats["mean"] = round(Stats.get_col_mean(col = self.data[col]),2)
                stats["median"] = round(Stats.get_col_median(col = self.data[col]),2)
                stats["mode"] = round(Stats.get_col_mode(col = self.data[col]),2)
            else:
                stats["min"] = "_"
                stats["max"] = "_"
                stats["mean"] = "_"
                stats["median"] = "_"
                stats["mode"] = Stats.get_col_mode(col = self.data[col])

            describe_dict[col] = stats

        # ---- printing ----
        print(f"{'Column':<15}{'Nulls':<10}{'Min':<10}{'Max':<10}{'Mean':<10}{'Median':<10}{'Mode':<10}")
        print("-" * 75)

        for col, stats in describe_dict.items():
            print(
                f"{col:<15}"
                f"{stats['nulls']:<10}"
                f"{stats['min']:<10}"
                f"{stats['max']:<10}"
                f"{stats['mean']:<10}"
                f"{stats['median']:<10}"
                f"{stats['mode']:<10}"
            )

    #TODO: define fillna()   
    def fillna(self, numeric_function: str = "mean", cate_function: str = "mode") ->None:
        col_nulls = self.count_nulls()

        for col, null_count in col_nulls.items():
            if null_count == 0:
                continue

            col_type = self.dtype[col]
            values = self.data[col]

            # ---- numeric columns ----
            if col_type in ("int", "float"):
                if numeric_function == "mean":
                    fill_value = Stats.get_col_mean(values)
                elif numeric_function == "median":
                    fill_value = Stats.get_col_median(values)
                elif numeric_function == "mode":
                    fill_value = Stats.get_col_mode(values)
                else:
                    raise ValueError("Numeric filling method doesn't exist")

            # ---- categorical columns ----
            elif col_type == "string":
                if cate_function == "mode":
                    fill_value = Stats.get_col_mode(values)
                else:
                    raise ValueError("Categorical filling method doesn't exist")

            else:
                raise TypeError(f"Unsupported data type: {col_type}")

            # ---- fill ----
            for i in range(len(values)):
                if values[i] is None:
                    values[i] = fill_value


    #TODO: define to_csv()
    def to_csv(self,path) -> None:
        File_Handler.write_file(file_path=path,data=self.data)


    def __str__(self):
        data = self.data
        columns = list(data.keys())
        n_rows = len(next(iter(data.values())))

        # 1) compute column widths
        widths = {}
        for col in columns:
            max_width = len(col)
            for val in data[col]:
                max_width = max(max_width, len(str(val)))
            widths[col] = max_width

        # 2) build header
        lines = []
        header = ""
        for col in columns:
            header += col.ljust(widths[col] + 2)
        lines.append(header.rstrip())

        # 3) separator
        sep = ""
        for col in columns:
            sep += "-" * widths[col] + "  "
        lines.append(sep.rstrip())

        # 4) rows
        for i in range(n_rows):
            row = ""
            for col in columns:
                row += str(data[col][i]).ljust(widths[col] + 2)
            lines.append(row.rstrip())

        return "\n".join(lines)
