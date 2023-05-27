import os
import pandas as pd
import logging as log

# SQL server connection
from sqlalchemy import inspect


def write_df_to_excel(df, file_path_excel, sheet_name):
    """
    Function overwrites excel file if it exists
    """  
    writer = pd.ExcelWriter(file_path_excel)
    df.to_excel(writer, sheet_name=sheet_name, index=False)
    writer.save()
    log.info(f'Data written to {file_path_excel}')
    
    
def append_df_to_csv(df, file_path_local):
    """
    Function appends DataFrame to csv file
    """
    if not os.path.isfile(file_path_local):
        df.to_csv(file_path_local, header='column_names', index=False, encoding='utf-8')
        log.info(f'Data written to {file_path_local}')
    else:
        # else it exists so append without writing the header
        df.to_csv(file_path_local, mode='a', header=False, index=False, encoding='utf-8')
        log.info(f'Data appended to {file_path_local}')
        
        
def drop_sql_table(engine, schema_ref, table_ref):
    """
    Drop table in the Microsoft SQL server database
    """
    with engine.connect() as connection:
        connection.execute(f'DROP TABLE {schema_ref}.{table_ref}')
        log.info(f'Table {schema_ref}.{table_ref} dropped')
        
def write_df_to_sql(engine, df, table_ref):
    """
    Write the DataFrame to a table in the Microsoft SQL server database
    """ 
    inspector = inspect(engine)
    
    if inspector.has_table(table_ref):
        drop_sql_table(engine, table_ref)
    df.to_sql(table_ref, con=engine, if_exists='append', index=False)
    log.info(f'Table {table_ref} written to database')