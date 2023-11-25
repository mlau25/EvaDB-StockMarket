import datetime
import time
import stock_analysis

user_commands = {
    'h': "List available commands",
    'e': "End session",
    'ls': "List stored stocks",
    'query': "Query the most recent price(s) of a stock",
    'forecast': "View price forecasts of a stock",
    'news': "Request a news digest for a stock",
    'compare': "Request a report on a comparison between two stocks",
    'analysis': "Request an analysis report on a specific stock",
}

def stock_query_session():
    
    setup()
    input("Press [Enter] to continue")

    introductions()

    while True:
        next_command = input("\nWhat would you like to do next\n    => ")

        if next_command in ['e', 'exit']:
            break
        
        elif next_command in ['h', 'help']:
            display_options()

        elif next_command in ['ls', 'list']:
            print(', '.join(stock_analysis.stocks))
        
        elif next_command == 'query':
            construct_user_query()

        elif next_command == 'forecast':
            execute_forecast()
        
        elif next_command == 'news':
            prepare_digest()
        
        elif next_command == 'analysis':
            prepare_analysis()

        elif next_command == 'compare':
            prepare_comparison()

        else:
            print("Invalid command, below are the supported user commands:")
            display_options


def setup():
    setup_mode = input("Before beginning, would you like to load a fresh setup ('fresh') or use a cached setup ('old'\[Enter])\n\t=> ").lower()
    while setup_mode not in ['fresh', 'old', '']:
        setup_mode = input("Invalid input\n  Type 'fresh' for a new setup\n  Type 'old' or Enter to use the cached setup\n\t=> ")
    
    time.sleep(0.2)
    if setup_mode in ['old', '']:
        print("-- -- -- -- Using Cached Database -- -- -- --")
        return

    # fresh setup
    print("\n")
    print("-- -- -- -- Setting Up Database -- -- -- -- ")
    time.sleep(0.2)
    print("Connecting to Postgres:", end='\t')
    stock_analysis.reset_all_postgres()
    stock_analysis.reset_all_eva()
    print("DONE")
    time.sleep(0.2)
    print("Setting up Database:", end='\t')
    stock_analysis.setup_database()
    print("DONE")
    time.sleep(0.2)
    print("Initializing Table:", end='\t')
    stock_analysis.setup_tables()
    print("DONE")
    time.sleep(0.2)
    # print("Training Forecasting Functions:", end='\t')
    # stock_analysis.setup_ai_funcs()
    # print("DONE")

def introductions():
    print("-- -- -- -- -- -- -- -- -- -- -- -- -- --")
    print("\n")
    print(
        """Welcome! In this program you have the capabilities to:\t
        1. query recent stock prices
        2. query predictions for prices in the near future
        3. request stock news digests powered by ChatGPT
        """)
    print("The database backing this program holds 5 years of\nstock data for several stocks.\n\nThese stocks are:\n", ', '.join(stock_analysis.stocks))

    print("\nBelow is a list of commands available to you")
    display_options()

    print("")
    print("-- -- -- -- -- -- -- -- -- -- -- -- -- --")
        
    input("Press [Enter] to continue")
    print("\nLet's begin!")
    time.sleep(0.2)

def display_options():
    print("User commands:")
    for command in user_commands:
        print(command, '\t', '->', user_commands[command])
    print("-- (More commands on the way!) --")

def construct_user_query():
    # gather user input on desired stock and recency
    stock = input("Choose a stock (input ticker):\n\t=> ").upper()
    while stock not in stock_analysis.stocks:
        print("Stock ticker is not currently stored, try again.")
        stock = input("Choose a stock (ticker):\n\t=> ").upper()
    num_days = input("Number of recent closing prices (or 'today'):\n\t=> ").lower()
    while (num_days != 'today' and not num_days.isdigit()) or (num_days.isdigit() and int(num_days) <= 0):
        print("Invalid request, try again.")
        num_days = input("Type a positive integer (or 'today'):\n\t=> ").lower()
    if num_days == 'today': num_days = '1'
    
    queryDF = stock_analysis.recent_price_query(stock, num_days).sort_values('date', ascending=False)
    print(queryDF)

def execute_forecast():
    # gather user input on desired stock and forecast horizon
    stock = input("Choose a stock (input ticker):\n\t=> ").upper()
    while stock not in stock_analysis.stocks:
        print("Stock ticker is not currently stored, try again.")
        stock = input("Choose a stock (ticker):\n\t=> ").upper()
    horizon = input("Number of projected days (or 'tomorrow'):\n\t=> ").lower()
    while (horizon != 'tomorrow' and not horizon.isdigit()) or (horizon.isdigit() and int(horizon) <= 0):
        print("Invalid request, try again.")
        horizon = input("Type a positive integer (or 'tomorrow'):\n\t=> ").lower()
    if horizon == 'tomorrow': horizon = 1
    indicator = input("Forecasted metric ('open', 'close'\[Enter], 'low', 'high'):\n\t=> ").lower()
    while indicator not in ['open', 'close', '', 'low', 'high']:
        indicator = input("Choose between ('open', 'close'\[Enter], 'low', 'high'):\n\t=> ").lower()
    if indicator == '': indicator = 'close'

    queryDF = stock_analysis.forecast_price(stock, horizon, indicator)
    print(queryDF)

def prepare_digest():
    news_digest = stock_analysis.create_digest()
    print(news_digest)

def prepare_analysis():
    # gather user input on desired stock to generate news digest on
    stock = input("Choose a stock (input ticker):\n\t=> ").upper()
    while stock not in stock_analysis.stocks:
        print("Stock ticker is not currently stored, try again.")
        stock = input("Choose a stock (ticker):\n\t=> ").upper()

    report = stock_analysis.create_analysis_report(stock)
    print(report)

def prepare_comparison():
    # gather user input on desired stocks to generate comparison report on
    stock1 = input("Choose a first stock (input ticker):\n\t=> ").upper()
    while stock1 not in stock_analysis.stocks:
        print("Stock ticker is not currently stored, try again.")
        stock1 = input("Choose a first stock (ticker):\n\t=> ").upper()
    stock2 = input("Choose a second stock (input ticker):\n\t=> ").upper()
    while stock2 not in stock_analysis.stocks or stock2 == stock1:
        if stock2 not in stock_analysis.stocks:
            print("Stock ticker is not currently stored, try again.")
        else:
            print(f"Second stock cannot be the same as the first ({stock1})")
        stock2 = input("Choose a second stock (ticker):\n\t=> ").upper()

    report = stock_analysis.create_comparison_report(stock1, stock2)
    print(report)

if __name__ == '__main__':
    stock_query_session()
    print("\n\nYour session has ended! \n\nThank you :)")
