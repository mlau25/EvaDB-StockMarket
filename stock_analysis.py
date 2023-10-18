import os
import pandas as pd
import evadb
import psycopg2


db_params = {
    'user': 'postgres',
    'password': 'bbqporkbun25$',
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres'
}

stocks = ['AAPL', 'AMD', 'AMZN', 'GOOGL', 'INTC', 'JPM', 'MA', 'META', 'MSFT', 'NVDA', 'TSLA', 'V']

def reset_all_postgres():
    drop_table = "DROP TABLE IF EXISTS stock_data;"
    create_table = """
    CREATE TABLE stock_data (
        stock_symbol varchar,
        date varchar,
        open numeric,
        close numeric,
        high numeric,
        low numeric,
        volume bigint
    );
    """

    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    cursor.execute(drop_table)
    cursor.execute(create_table)
    connection.commit()

    cursor.close()
    connection.close()

def reset_all_eva():
    drop_table = """
        USE postgres_stock_data {
            DROP TABLE IF EXISTS stock_table
        }
    """

    drop_db = "DROP DATABASE IF EXISTS postgres_stock_data;"

    cursor = evadb.connect().cursor()
    cursor.query(drop_table).df()
    cursor.query(drop_db).df()
    cursor.close()

def setup_database():
    conditional_drop = "DROP DATABASE IF EXISTS postgres_stock_data;"
    connect_postgres = f"CREATE DATABASE postgres_stock_data WITH ENGINE = 'postgres', PARAMETERS = {db_params};"

    cursor = evadb.connect().cursor()

    cursor.query(conditional_drop)
    cursor.query(connect_postgres).df()

def setup_tables():
    drop_table = """
        USE postgres_stock_data {
            DROP TABLE IF EXISTS stock_table
        }
    """

    create_stock_table = """
    USE postgres_stock_data {
        CREATE TABLE IF NOT EXISTS stock_data (stock_symbol TEXT, date DATE, open NUMERIC, close NUMERIC, high NUMERIC, low NUMERIC, volume BIGINT)
    }
    """
    
    cursor = evadb.connect().cursor()
    cursor.query(drop_table).df()
    cursor.query(create_stock_table).df()
    cursor.close()

    upload_stock_data(merge_stock_data())

def recent_price_query(ticker: str, num_days: str):
    cursor = evadb.connect().cursor()

    query = f"""
    USE postgres_stock_data {{
        SELECT stock_symbol, date, close
        FROM stock_data
        WHERE stock_symbol = '{ticker}'
        ORDER BY date DESC
        LIMIT {num_days}
    }}
    """

    df = cursor.query(query).df()
    cursor.close()

    return df

def forecast_price(ticker: str, horizon: str, indicator: str = 'close'):
    train_forecast_func(ticker, indicator)

    cursor = evadb.connect().cursor()

    df = cursor.query(f"SELECT Forecast{ticker}({horizon}) ORDER BY DATE DESC;").df()

    return df

def train_forecast_func(ticker: str, indicator: str = 'close'):
    cursor = evadb.connect().cursor()

    conditional_drop = f"DROP FUNCTION IF EXISTS Forecast{ticker}"

    cursor.query(conditional_drop).df()
    cursor.query(f"""
        CREATE FUNCTION Forecast{ticker} FROM
            (
                SELECT stock_symbol, date, {indicator}
                FROM postgres_stock_data.stock_data
                WHERE stock_symbol = '{ticker}'
            )
        TYPE Forecasting
        PREDICT '{indicator}'
        TIME 'date'
        ID 'stock_symbol'
        MODEL 'AutoTheta'
        FREQUENCY 'D'
    """).df()

    cursor.close()

def setup_ai_funcs():
    cursor = evadb.connect().cursor()
    
    conditional_drop = "DROP FUNCTION IF EXISTS StockPriceForecast;"

    cursor.query(conditional_drop).df()
    cursor.query("""
        CREATE FUNCTION StockPriceForecast FROM
            (
                SELECT stock_symbol, date, close
                FROM postgres_stock_data.stock_data
                WHERE stock_symbol = 'GOOG'
            )
        TYPE Forecasting
        PREDICT 'close'
        TIME 'date'
        ID 'stock_symbol'
        MODEL 'AutoTheta'
        FREQUENCY 'D'
    """).df()

    cursor.close()

# merges multiple stock .csv files into one
def merge_stock_data():
    merged_data = pd.DataFrame()

    stock_folder_path = 'stonkprices'

    for ticker in stocks:
        stock_data_filepath = os.path.join(stock_folder_path, f'{ticker}.csv')
        stock_data = pd.read_csv(stock_data_filepath)

        stock_data.columns = stock_data.columns.str.lower() # convert column names to lower-case
        stock_data['stock_symbol'] = ticker # add ticker symbol attribute

        merged_data = merged_data._append(stock_data)
    
    return merged_data

# upload merged data to postgres
def upload_stock_data(merged_data: pd.DataFrame):
    # SQL command to insert fresh data
    cursor = evadb.connect().cursor()


    # # Insert fresh data
    # cursor.copy_expert(sql=insert_data_sql, file=merged_data.to_csv(index=False, sep=','))
    # connection.commit()

    # iteratively insert data row by row
    for idx, row in merged_data.iterrows():
        insert_row = f"""
        USE postgres_stock_data {{
        INSERT INTO stock_data VALUES ('{row['stock_symbol']}', '{row['date']}', '{row['open']}', '{row['close']}', '{row['high']}', '{row['low']}', '{row['volume']}')
        }}
        """
        cursor.query(insert_row).df()

    cursor.close()

# if __name__ == '__main__':
#     reset_all_eva()

#     setup_database()
#     setup_tables()
#     setup_ai_funcs()
#     print("donejamin")
#     # init_price_table()
