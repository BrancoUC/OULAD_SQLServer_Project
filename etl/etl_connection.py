from sqlalchemy import create_engine

def connect_sql_server(server='localhost', database='oulad_db'):
    connection_string = (
        f"mssql+pyodbc://@{server}/{database}"
        "?driver=ODBC+Driver+17+for+SQL+Server"
        "&Trusted_Connection=yes"
    )
    engine = create_engine(connection_string)
    return engine
