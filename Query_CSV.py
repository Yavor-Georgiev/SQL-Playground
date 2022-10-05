import pandas as pd
import sqlalchemy as sa

print('\nCreated by:\n')
print('\tYAVOR GEORGIEV')
print('\tGitHub: https://github.com/Yavor-Georgiev/')
print('\t2022\n')

print('Version: ', pd.__version__, '(Pandas)')
print('\t ', sa.__version__, '(SQL Alchemy)')

print('\n')

if __name__=='__main__':

    #Create in-memory sqlite database
    engine = sa.create_engine('sqlite://')
    #Create in-file sqlite db
    #engine = sa.create_engine(r'sqlite:///C:\...PATH...\FILENAME.db')

    #Read CSV file to pandas dataframe
    df = pd.read_csv('C:\Projects_temp\CarSales.csv')

    #print(df.shape)

    #Write the pandas dataframe to specific table in the connected db and
    #drop the build-in dataframe index
    df.to_sql(con=engine, name='Table1', if_exists='replace', index=False)

    #Construct the SQL querry 
    sql_query = '''
        SELECT * FROM Table1 ORDER BY 1 ASC limit 3
    '''

    #Execute querry on the connected database
    with engine.connect().execution_options(autocommit=True) as conn:
        exe = conn.execute(sa.sql.text(sql_query))
        
        #Get all results
        result = exe.fetchall()
        
        #Print all results
        for r in result:
            print(r)

    #result_df = pd.DataFrame(result)