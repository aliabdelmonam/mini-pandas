from file_handler import File_Handler
from stats import Stats
from functools import reduce

class Dataframe:
    def __init__(self, data:dict, dtype:dict):
        self.data = data
        self.dtype = dtype
    
    def read_csv(self, data_path: str, dtype_path: str) -> None:
        """
        Reads a CSV file and its corresponding dtype file, and initializes the Dataframe object with the data and dtype.

        Args:
            data_path (str): Path to the CSV data file.
            dtype_path (str): Path to the CSV dtype file.
        
        Return:
            None
        """
        self.dtype = File_Handler.read_dtype(dtype_path)
        self.data = File_Handler.read_csv_file(data_path, self.dtype)
    
    #TODO: define count_nulls()
    def count_nulls(self):
        dict_nulls={}
        for col,val in self.data.items():

            dict_nulls[col] = reduce(lambda cnt,y: cnt+ (1 if y is None else 0),val,0)
        
        return dict_nulls
    
    #TODO: define describe()
    def describe(self)->None:
        describe_dict = {}
        function_names = ['column','Nulls','max','min','mean','median','mode']
        for col,data_type in self.dtype:
            ll =[]
            if data_type =='int':

                ll.append(Stats.get_col_nulls(self.data[col]))
                ll.append(Stats.get_col_max(self.data[col]))
                ll.append(Stats.get_col_min(self.data[col]))
                ll.append(Stats.get_col_mean(self.data[col]))
                ll.append(Stats.get_col_median(self.data[col]))
                ll.append(Stats.get_col_mode(self.data[col]))

                describe_dict[col] = ll
            else:
                ll.append(Stats.get_col_nulls(self.data[col]))
                ll.append(Stats.get_col_mode(self.data[col]))

                describe_dict[col] = ll
        
        for name in function_names:
            print(name)
        
        print(f"{'Column':<15}{'Min':<10}{'Max':<10}{'Nulls':<10}")
        print("-" * 45)

        for col, values in self.data.items():
            null_count = self.count_nulls()[col]
            if self.dtype[col] in ['int', 'float']:
                col_min = min(filter(None, values))  # Exclude None values
                col_max = max(filter(None, values))
            else:
                col_min = col_max = 'N/A'

            print(f"{col:<15}{col_min:<10}{col_max:<10}{null_count:<10}")
        


    #TODO: define fillna()   
    def fillna(self, numeric_function: str = "mean", cate_function: str = "mode"):
        col_nulls = self.count_nulls()

        for col, val in col_nulls.items():
            if val > 0:  # we need to fill null here
                t_type = self.dtype[col]

                if t_type == 'int' or t_type == 'float':
                    t_val = self.data[col]
                    for i in range(len(t_val)):
                        if t_val[i] is None:
                            if numeric_function == 'mean':
                                t_val[i] = Stats.get_col_mean(t_val)
                            elif numeric_function == 'median':
                                t_val[i] = Stats.get_col_median(t_val)
                            elif numeric_function == 'mode':
                                t_val[i] = Stats.get_col_mode(t_val)
                            else:
                                raise Exception("Filling Method Doesn't Exist")

                elif t_type == 'str':
                    t_val = self.data[col]
                    for i in range(len(t_val)):
                        if t_val[i] is None:
                            if cate_function == 'mode':
                                t_val[i] = Stats.get_col_mode(t_val)
                            else:
                                raise Exception("Filling Method Doesn't Exist")

                else:
                    raise Exception("Unsupported data type for fillna")

    #TODO: define to_csv()
        def to_csv(self) -> None:
            File_Handler.write_file(file_path='AFFFFTER.csv',data=self.data)






