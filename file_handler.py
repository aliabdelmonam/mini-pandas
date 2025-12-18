import csv
class File_Handler:

    @classmethod
    def read_dtype(self,file_path):
        """
        Read a CSV file containing column names and their data types.

        Args:
            file_path (str): Path to the CSV file containing column names and types.

        Returns:
            dict: A dictionary where keys are column names and values are data types ('int', 'float', 'string').
        """
        data_types= {}
        try:
            with open(file_path) as file:
                for line in csv.DictReader(file, delimiter=','):
                    data_types[line['column']]=line['dtype']
        except Exception as e:
            print(f"Could read:{e}")

        return data_types

    @classmethod
    def read_csv_file(self,file_path, dtypes:dict) -> dict:
        """
        Read a CSV file and convert each column to the specified data type.

        Args:
            file_path (str): Path to the CSV data file.
            dtypes (dict): Dictionary mapping column names to data types ('int', 'float', 'string').

        Returns:
            dict: A dictionary where keys are column names and values are lists of column values.
                Missing values (empty strings) are replaced with None.
        
        """
        data_dict={}
        data_types = self.read_dtype('D:\ITI\Python for ML\Lab\mini-project\starter_code\data\titanic_dtype.csv')

        try:
            with open(file_path, 'r') as csv_file:
                spamreader = csv.DictReader(csv_file, delimiter=',')
                
                for row in spamreader:
                    for key, val in row.items():

                        if key not in data_dict:
                            data_dict[key] = []
                        data_dict[key].append(val)
                        
        except Exception as e:
            print(f"Couldn't load the file: {e}")

        

        try:

            for column, list_val in data_dict.items():
                if data_types[column]=='int':
                    data_dict[column] = [int(val) if val.strip() else None for val in list_val]
                elif data_types[column]=='float':
                    data_dict[column] = [float(val) if val.strip() else None for val in list_val]
                elif data_types[column]=='string':
                    data_dict[column] = [str(val) if val.strip() else None for val in list_val]
        except Exception as e:
            print(f"Couldn't Convert Data type: {e}")

        return data_dict  
       
    @classmethod
    def write_file(self,file_path, data:dict) -> None:
        """
        Write a data dictionary to a CSV file.

        Args:
            file_path (str): Path to the output CSV file.
            data (dict): Dictionary where keys are column names and values are lists of column values.

        Returns:
            None
        """

        with open(file_path, 'w', newline='') as csvfile:
            fieldnames = list(data.keys())
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
        
            num_rows = len(next(iter(data.values())))
            
            for i in range(num_rows):
                row = {key: data[key][i] for key in fieldnames}
                writer.writerow(row)

        return None