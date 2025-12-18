from DataFrame import Dataframe
import Stats

def main():
    # TODO: Read data

    df = Dataframe.read_csv(data_path=r'D:\ITI\Python for ML\Lab\mini-project\starter_code\data\titanic.csv'
                ,dtype_path=r"D:\ITI\Python for ML\Lab\mini-project\starter_code\data\titanic_dtype.csv")
    
    df = Dataframe(data=df.data, dtype=df.dtype)

   
    
    df.describe()
    # TODO: Fill missing values
    # df.fillna(numeric_function='mean',cate_function='mode')
    # Numeric columns → mean
    # Categorical columns → mode
    # df.describe()
    # print(df.data)


    # TODO:Write cleaned data to CSV

    df.to_csv(path='lol.csv')

if __name__ == "__main__":
    main()
