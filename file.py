import os
import pandas as pd
# from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import requests
 
# Fetch username and password from environment variables
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")

print(username)
print(password)
 
# Define MySQL connection parameters
# mysql_user = os.getenv('MYSQL_USER', 'root')
# mysql_password = os.getenv('MYSQL_PASSWORD', 'root')
# mysql_host = '192.168.3.112'
# mysql_database = os.getenv('MYSQL_DATABASE', 'my_db')
 
# Create SQLAlchemy engine for MySQL
# engine = create_engine(f'mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}')
 
# Fetch data
session = requests.Session()
login_url = "https://www.screener.in/login/?"
login_page = session.get(login_url)
soup = BeautifulSoup(login_page.content, 'html.parser')
 
# Find the CSRF token
csrf_token = soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']
login_payload = {
    'username': username,
    'password': password,
    'csrfmiddlewaretoken': csrf_token
}
 
headers = {
    'Referer': login_url,
    'User-Agent': 'Mozilla/5.0'
}
 
# Perform login
response = session.post(login_url, data=login_payload, headers=headers)
print(f"Login response URL: {response.url}")
response.url = "https://www.screener.in/dash/"
if response.url == "https://www.screener.in/dash/":
    search_url = "https://www.screener.in/company/RELIANCE/consolidated/"
    search_response = session.get(search_url)
   
    if search_response.status_code == 200:
        print("Reliance data retrieved successfully")
        soup = BeautifulSoup(search_response.content, 'html.parser')
        # table = soup.find('table', {'class': 'data-table responsive-text-nowrap'})
        table1 = soup.find('section', {'id': 'profit-loss'})
        table = table1.find('table')
       
        if table:
            headers = [th.text.strip() or f'Column_{i}' for i, th in enumerate(table.find_all('th'))]
            rows = table.find_all('tr')
            row_data = []
           
            for row in rows[1:]:
                cols = row.find_all('td')
                cols = [col.text.strip() for col in cols]
                if len(cols) == len(headers):
                    row_data.append(cols)
                else:
                    print(f"Row data length mismatch: {cols}")
           
            # Create a DataFrame with sanitized headers
            df = pd.DataFrame(row_data, columns=headers)
            # Rename the first column to 'Narration'
            if not df.empty:
                df.columns = ['Narration'] + df.columns[1:].tolist()
            # Drop the index column if it exists
            df = df.reset_index(drop=True)
            # Print the DataFrame columns and the first few rows for debugging
            print(df.head())
           
            # Load DataFrame into MySQL
            # try:
            #     df.to_sql('test', con=engine, if_exists='replace', index=False)
            #     print("Data successfully loaded into MySQL.")
            # except Exception as e:
            #     print(f"Error loading data into MySQL: {e}")
        else:
            print("Failed to find the data table.")
    else:
        print(f"Failed to retrieve Reliance data. Status Code: {search_response.status_code}")
else:
    print("Login failed.")