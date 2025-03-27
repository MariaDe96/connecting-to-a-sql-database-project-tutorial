import os 
import pandas as pd 
from sqlalchemy import create_engine, text 
from dotenv import load_dotenv 

load_dotenv()

def connect():
    global engine
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print(f"Connection string: {connection_string}")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        with engine.connect() as connection:
            print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

engine = connect()
if engine is None:
    print("Error: Unable to connect to the database. Exiting...")
    exit()

def create_tables(engine):
 try:
    with engine.connect() as connection:
             connection.execute(text("""
        CREATE TABLE IF NOT EXISTS publishers (
           publisher_id SERIAL PRIMARY KEY,
           name VARCHAR(255) NOT NULL
      );

        CREATE TABLE IF NOT EXISTS authors (
          author_id SERIAL PRIMARY KEY,
          first_name VARCHAR(100) NOT NULL,
          middle_name VARCHAR(50) NULL,
          last_name VARCHAR(100) NULL
      );

        CREATE TABLE IF NOT EXISTS books (
          book_id SERIAL PRIMARY KEY,
          title VARCHAR(255) NOT NULL,
          total_pages INT NULL,
          rating DECIMAL(4, 2) NULL,
          isbn VARCHAR(13) NULL,
          published_date DATE,
          publisher_id INT NULL,
          CONSTRAINT fk_publisher FOREIGN KEY (publisher_id) REFERENCES publishers(publisher_id) ON DELETE SET NULL
      );

        CREATE TABLE IF NOT EXISTS book_authors (
          book_id INT NOT NULL,
          author_id INT NOT NULL,
          PRIMARY KEY (book_id, author_id),
          CONSTRAINT fk_book FOREIGN KEY (book_id) REFERENCES books(book_id) ON DELETE CASCADE,
          CONSTRAINT fk_author FOREIGN KEY (author_id) REFERENCES authors(author_id) ON DELETE CASCADE
       );
    """))
    print("Tables created successfully!")
 except Exception as e:
    print(f"Error creating tables: {e}")
    
def insert_data(engine):
    try:
       with engine.connect() as connection:
            connection.execute(text("""
            INSERT INTO publishers (publisher_id, name) VALUES
            (1, 'O Reilly Media'),
            (2, 'A Book Apart'),
            (3, 'A K PETERS'),
            (4, 'Academic Press'),
            (5, 'Addison Wesley'),
            (6, 'Albert&Sweigart'),
            (7, 'Alfred A. Knopf')
            ON CONFLICT (publisher_id) DO NOTHING;

            INSERT INTO authors (author_id, first_name, middle_name, last_name) VALUES
            (1, 'Merritt', NULL, 'Eric'),
            (2, 'Linda', NULL, 'Mui'),
            (3, 'Alecos', NULL, 'Papadatos'),
            (4, 'Anthony', NULL, 'Molinaro'),
            (5, 'David', NULL, 'Cronin'),
            (6, 'Richard', NULL, 'Blum'),
            (7, 'Yuval', 'Noah', 'Harari'),
            (8, 'Paul', NULL, 'Albitz')
             ON CONFLICT (author_id) DO NOTHING;
    """))
       print("Data inserted successfully!")
    except Exception as e:
             print(f"Error inserting data: {e}")

def read_table(engine):
    try:
        df = pd.read_sql_query("SELECT * FROM publishers;", engine)
        if df.empty:
            print("No data found in the publishers table.")
        else:
            print(df)
    except Exception as e:
        print(f"Error reading publishers table: {e}")
engine = connect()
if engine is None:
    exit()
create_tables(engine)
insert_data(engine)
read_table(engine)