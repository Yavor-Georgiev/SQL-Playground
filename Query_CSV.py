from pandas import read_csv, DataFrame
from sqlalchemy import create_engine, sql

print('\nCreated by:\n')
print('\tYAVOR GEORGIEV')
print('\tGitHub: https://github.com/Yavor-Georgiev/')
print('\t2022\n')

if __name__=='__main__':

    #Create in-memory sqlite database
    engine = create_engine('sqlite://')

#Create in-file sqlite db
    #engine = sa.create_engine(r'sqlite:///C:\...PATH...\FILENAME.db')

    #Read CSV file to pandas dataframe
    in_df = read_csv('/storage/emulated/0/documents/Northwind_csv-main/us_states.csv')

    print('Input table has', in_df.shape[0], 'rows and', in_df.shape[1], 'columns\n')

    #Write the pandas dataframe to specific table in the connected db and
    #drop the build-in dataframe index
    in_df.to_sql(con=engine, name='Table1', if_exists='replace', index=False)

    #Construct the SQL querry 
    sql_query = '''
        SELECT * FROM Table1 ORDER BY 1 ASC limit 3
    '''

    #Execute querry on the connected database
    with engine.connect().execution_options(autocommit=True) as conn:
        exe = conn.execute(sql.text(sql_query))
        
        #Get all results
        result = exe.fetchall()
        
        #Print all results
        for r in result:
            print(r)

    out_df = DataFrame(result)
    
    out_df.to_csv('/storage/emulated/0/documents/Northwind_csv-main/result1.csv', header=False, index=False)
    
    print('\n', out_df)