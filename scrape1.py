import os
import pandas as pd
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
import time

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

def login_and_fetch_data(driver, username, password):
    """Login and fetch data using Selenium."""
    driver.get("https://www.screener.in/login/?")
    
    # Input the username and password
    username_input = driver.find_element(By.ID, 'id_username')
    password_input = driver.find_element(By.ID, 'id_password')
    
    username_input.send_keys(username)
    password_input.send_keys(password)
    
    # Submit the login form
    password_input.send_keys(Keys.RETURN)
    
    # Wait for login to complete and redirect
    time.sleep(3)  # Adjust time based on the website speed
    
    # Check if login was successful
    if driver.current_url == "https://www.screener.in/dash/":
        print("Login successful.")
        
        # Navigate to the data page
        driver.get("https://www.screener.in/company/RELIANCE/consolidated/")
        
        # Wait for the page to load
        time.sleep(3)
        
        # Find the table within the 'Profit & Loss' section
        table = driver.find_element(By.CSS_SELECTOR, 'section#profit-loss table')
        
        # Extract headers
        headers = [header.text.strip() for header in table.find_elements(By.CSS_SELECTOR, 'th')]
        
        # Extract rows of data
        data = []
        rows = table.find_elements(By.CSS_SELECTOR, 'tr')
        for row in rows[1:]:
            cols = [col.text.strip() for col in row.find_elements(By.CSS_SELECTOR, 'td')]
            if len(cols) == len(headers):
                data.append(cols)
            else:
                print(f"Row data length mismatch: {cols}")
        
        # Create DataFrame
        df = pd.DataFrame(data, columns=headers)
        if not df.empty:
            df.columns = ['Narration'] + df.columns[1:].tolist()
        return df
    else:
        print("Login failed.")
        return None

def save_to_csv(df, file_path):
    """Save DataFrame to CSV file."""
    df.to_csv(file_path, index=False)
    print(f"Data successfully saved to CSV: {file_path}")

def load_to_postgres(df, engine, table_name):
    """Load DataFrame into PostgreSQL."""
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print("Data successfully loaded into PostgreSQL.")
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")

def main():
    """Main function to execute the script."""
    username = 'rahul.acharya@godigitaltc.com'
    password = 'st0cksrahul@1'
    print(username, password)
    
    # Create PostgreSQL engine
    engine = create_pg_engine()
    if not engine:
        return
    
    # Setup Selenium WebDriver
    options = Options()
    options.headless = True  # Set headless if you don't need a browser window
    driver = webdriver.Chrome(options=options)
    
    try:
        # Login and fetch data
        df = login_and_fetch_data(driver, username, password)
        
        if df is not None:
            csv_file_path = "reliance_data10.csv"
            save_to_csv(df, csv_file_path)
            load_to_postgres(df, engine, 'reliance_data10')
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
