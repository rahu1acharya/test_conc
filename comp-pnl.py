import os
import pandas as pd
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import requests

def create_pg_engine():
    """Create a SQLAlchemy engine for PostgreSQL."""
    try:
        pg_user = os.getenv('PG_USER', 'concourse_user')
        pg_password = os.getenv('PG_PASSWORD', 'concourse_pass')
        pg_host = '192.168.56.1'
        pg_database = os.getenv('PG_DATABASE', 'concourse')
        pg_port = os.getenv('PG_PORT', '5432')

        engine = create_engine(f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_database}')
        print("PostgreSQL engine created successfully.")
        return engine
    except Exception as e:
        print(f"Error creating PostgreSQL engine: {e}")
        return None

def fetch_login_csrf_token(session, login_url):
    """Fetch CSRF token from the login page."""
    login_page = session.get(login_url)
    soup = BeautifulSoup(login_page.content, 'html.parser')
    return soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']

def login(session, login_url, username, password, csrf_token):
    """Perform login and return session response."""
    login_payload = {
        'username': username,
        'password': password,
        'csrfmiddlewaretoken': csrf_token
    }
    headers = {
        'Referer': login_url,
        'User-Agent': 'Mozilla/5.0'
    }
    return session.post(login_url, data=login_payload, headers=headers)

def fetch_data(session, search_url):
    """Fetch data from the search URL and return BeautifulSoup object."""
    search_response = session.get(search_url)
    if search_response.status_code == 200:
        print(f"Data retrieved successfully from {search_url}.")
        return BeautifulSoup(search_response.content, 'html.parser')
    else:
        print(f"Failed to retrieve data from {search_url}. Status Code: {search_response.status_code}")
        return None

def parse_table(soup):
    """Parse the table from BeautifulSoup object and return DataFrame."""
    table = soup.find('section', {'id': 'balance-sheet'}).find('table')         #'id': 'profit-loss'
    if table:
        headers = [th.text.strip() or f'Column_{i}' for i, th in enumerate(table.find_all('th'))]
        rows = table.find_all('tr')
        row_data = []

        for row in rows[1:]:
            cols = [col.text.strip() for col in row.find_all('td')]
            if len(cols) == len(headers):
                row_data.append(cols)
            else:
                print(f"Row data length mismatch: {cols}")

        df = pd.DataFrame(row_data, columns=headers)
        if not df.empty:
            df.columns = ['Narration'] + df.columns[1:].tolist()
        df = df.reset_index(drop=True)
        return df
    else:
        print("Failed to find the data table.")
        return None

def save_to_transposed_csv(df, company_name, all_data_list):
    """Append transposed DataFrame to a list with company name."""
    if df is not None:
        df_transposed = df.set_index('Narration').T  # Transpose the DataFrame
        df_transposed.reset_index(inplace=True)
        df_transposed.rename(columns={'index': 'Date'}, inplace=True)  # Rename index column to 'Date'

        # Clean and convert columns to numeric (except 'Date')
        for col in df_transposed.columns:
            if col != 'Date':
                # Remove commas and percentage symbols, then convert to numeric
                df_transposed[col] = df_transposed[col].replace({',': '', '%': ''}, regex=True)
                df_transposed[col] = pd.to_numeric(df_transposed[col], errors='coerce')

        # Fill NaN values
        df_transposed = df_transposed.fillna(0)

        # Clean column names: lowercase, replace spaces and symbols with underscores
        df_transposed.columns = [col.lower().replace(' ', '_').replace('+', '').replace('%', 'percent') for col in df_transposed.columns]
        df_transposed.rename(columns=lambda x: x.strip(), inplace=True)

        # Add company name as a new column
        df_transposed['company_name'] = company_name

        all_data_list.append(df_transposed)  # Append DataFrame to the list

def load_to_postgres(df_transposed, engine, table_name):
    """Load transposed DataFrame into PostgreSQL."""
    try:
        df_transposed = df_transposed.fillna(0)
        df_transposed.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print("Data successfully loaded into PostgreSQL.")
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")

def main():
    """Main function to execute the script."""
    username = "rahul.acharya@godigitaltc.com"#os.getenv("USERNAME")
    password = "st0cksrahul@1"#os.getenv("PASSWORD")

    # List of companies to scrape
    companies = [
        "HINDUNILVR", "ITC", "JYOTHYLAB", "BRITANNIA", "TATACONSUM",
        "DABUR", "GODREJCP", "MARICO", "ZYDUSWELL", "EMAMILTD"
    ]

    all_data_list = []

    # Fetch CSRF token and login
    session = requests.Session()
    login_url = "https://www.screener.in/login/?"
    csrf_token = fetch_login_csrf_token(session, login_url)
    response = login(session, login_url, username, password, csrf_token)

    if response.url == "https://www.screener.in/dash/":
        for company in companies:
            search_url = f"https://www.screener.in/company/{company}/consolidated/"
            soup = fetch_data(session, search_url)

            if soup:
                df = parse_table(soup)
                if df is not None:
                    save_to_transposed_csv(df, company, all_data_list)
    
        # Concatenate all DataFrames and load into PostgreSQL
        if all_data_list:
            merged_df = pd.concat(all_data_list, ignore_index=True)
            engine = create_pg_engine()
            if engine:
                load_to_postgres(merged_df, engine, 'ten_comp_bs')
    else:
        print("Login failed.")

if __name__ == "__main__":
    main()
