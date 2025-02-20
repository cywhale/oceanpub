import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values
import os, re, html, time
from dotenv import load_dotenv

load_dotenv()
# Database configuration
DBUSER = os.getenv('DBUSER')
DBPASS = os.getenv('DBPASS')
DBHOST = os.getenv('DBHOST')
DBPORT = os.getenv('DBPORT')
DBNAME = os.getenv('DBNAME')
PUBTABLE = os.getenv('PUBTABLE')
DB_CONFIG = {
    'dbname': DBNAME,
    'user': DBUSER,
    'password': DBPASS,
    'host': DBHOST,
    'port': DBPORT,
}

CROSSREF_API_URL = "https://api.crossref.org/works"
BATCH_SIZE = 5  # Insert every 5 records to balance performance and safety
RATE_LIMIT = 1  # Seconds to wait between API requests

# Mapping of Chinese columns to English
COLUMN_MAPPING = {
    '學校單位': 'affiliationTW',
    '姓名': 'correspondingTW',
    '海研一號': 'OR1',
    '海研二號': 'OR2',
    '海研三號': 'OR3',
    '海研五號': 'OR5',
    '新海研1號': 'NOR1',
    '新海研2號': 'NOR2',
    '新海研3號': 'NOR3',
    '勵進': 'LEGEND',
    '新海研1號貴儀中心': 'MIC1',
    '新海研2號貴儀中心': 'MIC2',
    '新海研3號貴儀中心': 'MIC3',
    '海洋學門資料庫': 'ODB'
}

def clean_title(text):
    text = re.sub(r'<.*?>|\%', '', text)  # Remove HTML-like tags
    text = re.sub(r'\s+', ' ', text)  # Remove HTML-like tags
    text = re.sub(r'\\u2010|\u2013|\u2014|\s*(-)\s*', '', text)  # Convert Unicode hyphen to normal hyphen
    text = re.sub(r'\\u2018|\u2019|\u201C|\u201D|', '', text)    # Convert Unicode single or double quote
    text = re.sub(r'\\u00f1', 'ñ', text)  # Convert Unicode 'ñ'
    text = re.sub(r'\\u00d1', 'Ñ', text)  # Convert Unicode 'Ñ'
    text = re.sub(r'[^\w\s]|[,\.]|[。,\.\?]$', '', text)  # Remove special characters except spaces
    return text.lower().strip()


def format_title_for_db(title: str) -> str:
    """
    Cleans the title field before inserting into the database.
    - Removes HTML tags like <scp>...</scp>
    - Converts Unicode hyphens and special characters
    - Keeps original case formatting
    """
    if not title:
        return ""
    
    # Remove HTML tags
    title = re.sub(r'<.*?>', '', title)
    title = re.sub(r'\s+', ' ', title)

    # Convert Unicode hyphen to regular hyphen
    title = re.sub(r'\u2010|\u2013|\u2014|\s*(-)\s*', "-", title)
    
    # Convert special characters like La Ni\u00f1a → La Niña
    title = title.replace("\u00f1", "ñ").replace("\u00d1", "Ñ")
    
    # Convert Unicode single and double quotes
    title = title.replace("\u2018", "'").replace("\u2019", "'")  # Left & Right single quotes
    title = title.replace("\u201C", '"').replace("\u201D", '"')  # Left & Right double quotes

    # Decode any escaped HTML entities (e.g., &amp;, &lt;)
    title = html.unescape(title)

    # Escape single quotes for PostgreSQL
    return title.replace("'", "''").strip()

def extract_title(paper_info):
    """
    Extracts the title from a paper reference, handling different date formats and title endings.
    The title can end with:
    - Chinese period (。)
    - English period (.)
    - Comma (,) when followed by a capitalized word and not inside parentheses
    """
    paper_info = re.sub(r"[‘’“”']", '', paper_info)
 
    # Case 1: Year inside parentheses (any content after year until closing parenthesis is ignored)
    match = re.search(r'[（\(]\d{4}[^）\)]*[）\)][.,]?\s*(.*?)(?:[。.]|,(?![^(]*\))\s*[A-Z])', paper_info)
    if match:
        return match.group(1).strip()

    # Case 2: Year not inside parentheses
    match = re.search(r'\d{4}[.,]?\s*(.*?)(?:[。.]|,(?![^(]*\))\s*[A-Z])', paper_info)
    if match:
        return match.group(1).strip()

    return paper_info.strip()  # Default case: return full text if nothing matches

def fetch_crossref_info(paper_title, max_retries=3):
    """Fetch paper details from CrossRef API with rate limiting and retries."""
    params = {'query.title': paper_title, 'rows': 5}
    
    for attempt in range(max_retries):
        time.sleep(RATE_LIMIT)  # Wait to respect API rate limits
        
        response = requests.get(CROSSREF_API_URL, params=params)
        
        if response.status_code == 200:
            items = response.json().get('message', {}).get('items', [])
            for item in items:
                if clean_title(paper_title) == clean_title(item.get('title', [''])[0]):
                    return item
            print(f"No exact match found for: {paper_title}")
            return None
        
        elif response.status_code == 503:  # Service Unavailable (Rate Limited)
            print(f"CrossRef API rate limit exceeded, retrying... ({attempt + 1}/{max_retries})")
            time.sleep(30)  # Wait longer before retrying
        
        else:
            print(f"CrossRef API request failed for: {paper_title} (Status {response.status_code})")
            return None
    
    print(f"Failed to fetch '{paper_title}' after {max_retries} attempts. Skipping.")
    return None


def transform_data(row, crossref_data):
    published_date_parts = crossref_data.get('published-print', {}).get('date-parts', [['Unknown']])[0]
    if 'Unknown' in published_date_parts:
        published_date_parts = crossref_data.get('published-online', {}).get('date-parts', [['Unknown']])[0]
 
    published_date = "-".join(map(str, published_date_parts)) if 'Unknown' not in published_date_parts else 'Unknown'
    print("Title: ", crossref_data.get('title', [''])[0], " which published_date is: ", published_date)  
    published_year = int(published_date_parts[0]) if published_date_parts[0] != 'Unknown' else None
     
    data = {
        'DOI': crossref_data.get('DOI', ''),
        'title': format_title_for_db(crossref_data.get('title', [''])[0]),
        'firstAuthor': crossref_data.get('author', [{}])[0].get('given', '') + ' ' + crossref_data.get('author', [{}])[0].get('family', ''),
        'authors': ', '.join([f"{a.get('given', '')} {a.get('family', '')}" for a in crossref_data.get('author', [])]),
        'publisher': crossref_data.get('publisher', ''),
        'journal': crossref_data.get('short-container-title', [''])[0],
        'published_year': published_year,
        'published_date': published_date,
        'abstract': re.sub(r'<.*?>|Abstract', '', crossref_data.get('abstract', '')),
        'URL': f"https://doi.org/{crossref_data.get('DOI', '')}"
    }
    
    for zh_col, en_col in COLUMN_MAPPING.items():
        if zh_col in row:
            data[en_col] = bool(row[zh_col])
    
    data['affiliationTW'] = row.get('學校單位', '') if pd.notna(row.get('學校單位')) else ''
    data['correspondingTW'] = row.get('姓名', '') if pd.notna(row.get('姓名')) else ''
    
    return data

def create_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    create_table_query = """
    CREATE TABLE IF NOT EXISTS publications (
        DOI TEXT PRIMARY KEY,
        title TEXT,
        firstAuthor TEXT,
        authors TEXT,
        publisher TEXT,
        journal TEXT,
        published_year INT,
        published_date TEXT,
        abstract TEXT,
        URL TEXT,
        affiliationTW TEXT,
        correspondingTW TEXT,
        OR1 BOOLEAN,
        OR2 BOOLEAN,
        OR3 BOOLEAN,
        OR5 BOOLEAN,
        NOR1 BOOLEAN,
        NOR2 BOOLEAN,
        NOR3 BOOLEAN,
        LEGEND BOOLEAN,
        MIC1 BOOLEAN,
        MIC2 BOOLEAN,
        MIC3 BOOLEAN,
        ODB BOOLEAN
    );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

def doi_exists(doi):
    """ Check if DOI already exists in the database """
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM publications WHERE DOI = %s", (doi,))
    exists = cursor.fetchone() is not None
    cursor.close()
    conn.close()
    return exists

def insert_into_postgres(records):
    """Insert records into PostgreSQL in batches."""
    if not records:
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    query = """
    INSERT INTO publications (DOI, title, firstAuthor, authors, publisher, journal, published_year,
                              published_date, abstract, URL, affiliationTW, correspondingTW,
                              OR1, OR2, OR3, OR5, NOR1, NOR2, NOR3, LEGEND, MIC1, MIC2, MIC3, ODB)
    VALUES %s
    ON CONFLICT (DOI) DO NOTHING;
    """
    
    values = [[record[col] for col in records[0].keys()] for record in records]
    execute_values(cursor, query, values)
    
    conn.commit()
    cursor.close()
    conn.close()

def process_csv(file_path, BATCH_SIZE=5):
    create_table()
    df = pd.read_csv(file_path)
    batch_data = []
    
    for _, row in df.iterrows():
        paper_info = row['論文']
        paper_title = extract_title(paper_info)
        if not paper_title or "http" in paper_title:
            print(f"Skipping: Paper title no content or contains URL '{paper_title}'")
            continue
        crossref_data = fetch_crossref_info(paper_title)
        
        if crossref_data:
            doi = crossref_data.get('DOI')
            if not doi:
                print(f"Warning: Skipping paper '{paper_title}' due to missing DOI.")
                continue
            if doi_exists(doi):
                print(f"Warning: Skipping duplicate DOI '{doi}' already in database.")
                continue

            record = transform_data(row, crossref_data)
            insert_into_postgres([record])  # Insert immediately

            # Also store in batch in case of later processing
            batch_data.append(record)

            # Batch insert every N records
            if len(batch_data) >= BATCH_SIZE:
                insert_into_postgres(batch_data)
                batch_data = []

    # Final insert for remaining data
    if batch_data:
        insert_into_postgres(batch_data)


if __name__ == "__main__":
    process_csv('../data/missing_titles_fix3.csv')