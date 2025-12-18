from functools import reduce
class Stats:

    @classmethod
    def get_datatype(cls,col:list)->str:
        for item in col:
            if not item:
                continue
            if isinstance(item,int):
                return "int"
            elif isinstance(item,float):
                return "float"
            elif isinstance(item,str):
                return "str"
        return None
    
    @classmethod
    def get_col_max(cls,col:list) ->float:
        """
        Compute the maximum value of a numerical column.

        Args:
            col (list): A list of numerical values. `None` values are ignored.

        Returns:
            The maximum value in the column (numeric type).
        """
        dtype = cls.get_datatype(col)
        if dtype=='str':
            raise Exception("Can't get Max of String Column")
        return reduce(lambda x, y: y if x is None else (x if y is None or x > y else y), col)
    
    @classmethod
    def get_col_min(cls,col:list) ->float:
        """
        Compute the minimum value of a numerical column.

        Args:
            col (list): A list of numerical values. `None` values are ignored.

        Returns:
            The minimum value in the column (numeric type).
        """
        dtype = cls.get_datatype(col)
        if dtype=='str':
            raise Exception("Can't get Min of String Column")
        return reduce(lambda x, y: y if x is None else (x if y is None or x < y else y), col)

    @classmethod
    def get_col_mean(cls, col:list) -> float:
        """
        Compute the mean (average) value of a numerical column.

        Args:
            col (list): A list of numerical values. `None` values are ignored.

        Returns:
            The mean value of the column (float).
        """
        dtype = cls.get_datatype(col)
        if dtype=='str':
            raise Exception("Can't get Mean of String Column")
        
        sum  = reduce(lambda x,y:y if x is None else x if y is None else (x+y)  ,col)
        count  = reduce(lambda x,y: x+(0 if y is None else 1),col,0)
        return float(sum)/count 

    
    @classmethod
    def get_col_median(cls, col:list) -> float:
        """
        Compute the median value of a numerical column.

        Args:
            col (list): A list of numerical values. `None` values are ignored.

        Returns:
            The median value of the column (numeric type).
        """
        dtype = cls.get_datatype(col)
        if dtype=='str':
            raise Exception("Can't get Mean of String Column")
        
        t = sorted(col, key=lambda x: (x is None, x))
        count  = reduce(lambda x,y: x+(0 if y is None else 1),col,0)

        if count ==1: # single number
            return 1.0
        
        if count%2==0: # even
            median = (t[count//2] + t[(count//2) -1])//2
            if median:
                return median
            else:
                return 0.0
        else: # odd
            median = t[count//2]
            if median:
                return median
            else:
                return 0.0
    
    @classmethod
    def get_col_mode(cls,col:list)->float:
        """
        Compute the mode (most frequent value) of a column.

        Args:
            col (list): A list of values. `None` values are ignored.

        Returns:
            The mode value of the column. If multiple values have the same
            frequency, the first encountered is returned.
        """
        
        count  = {}

        for val in col:
            if val is None:
                continue
            if val in count:
                count[val]+=1
            else:
                count[val]=1
        
        t= sorted(count.items(),key = lambda x:x[1],reverse=True)

        return t[0][0]

    @classmethod
    def get_col_nulls(cls,col:list)->int:

        return reduce(lambda cnt,x: cnt + (1 if x is None else 0),col,0)

    @classmethod  
    def get_stat(cls, data:dict, dtypes:dict, function:str)->dict:
        """
        Apply a statistical function to all numerical columns in a dataset.

        Args:
            data (dict): Dictionary where keys are column names and values are lists of column values.
            dtypes (dict): Dictionary where keys are column names and values are data types ('int', 'float', 'string').
            function (function): A function to apply to each numerical column (e.g., get_col_max, get_col_mean).

        Returns:
            dict: A dictionary where keys are column names and values are the result
            of applying the function to that column. Only numerical columns are processed.
        """
        func_dict = {}
        for column,val in data.items():
            if (dtypes[column]=='str') and (function !='mode'):
                continue
            elif (dtypes[column]=='str') and (function =='mode'):
                func_dict[column] = cls.get_col_mode(val)
            else:
                if function.lower() =='min':
                    func_dict[column] = cls.get_col_min(val)

                elif function.lower() =='max':
                    func_dict[column] = cls.get_col_max(val)

                elif function.lower() =='median':
                    func_dict[column] = cls.get_col_median(val)
                
                elif function.lower() =='mean':
                    func_dict[column] = cls.get_col_mean(val)
                
                elif function.lower() =='mode':
                    func_dict[column] = cls.get_col_mode(val)
                
        return func_dict
                    





