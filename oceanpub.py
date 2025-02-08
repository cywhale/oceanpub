import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values
import re

# Database configuration
DB_CONFIG = {
    'dbname': 'your_dbname',
    'user': 'postgres',
    'password': 'your_password',
    'host': 'localhost',
    'port': '5432'
}

CROSSREF_API_URL = "https://api.crossref.org/works"

# Mapping of Chinese columns to English
COLUMN_MAPPING = {
    '出版年份': 'published_year',
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
    return re.sub(r'[^\w\s]', '', text).lower()

def fetch_crossref_info(paper_title):
    params = {'query.title': paper_title, 'rows': 5}
    response = requests.get(CROSSREF_API_URL, params=params)
    if response.status_code == 200:
        items = response.json().get('message', {}).get('items', [])
        for item in items:
            if clean_title(paper_title) == clean_title(item.get('title', [''])[0]):
                return item
    return None

def transform_data(row, crossref_data):
    data = {
        'DOI': crossref_data.get('DOI', ''),
        'title': crossref_data.get('title', [''])[0],
        'firstAuthor': crossref_data.get('author', [{}])[0].get('given', '') + ' ' + crossref_data.get('author', [{}])[0].get('family', ''),
        'authors': ', '.join([f"{a.get('given', '')} {a.get('family', '')}" for a in crossref_data.get('author', [])]),
        'publisher': crossref_data.get('publisher', ''),
        'journal': crossref_data.get('short-container-title', [''])[0],
        'published_year': crossref_data.get('published-print', {}).get('date-parts', [[None]])[0][0],
        'abstract': re.sub(r'<.*?>', '', crossref_data.get('abstract', '')),
        'URL': f"https://doi.org/{crossref_data.get('DOI', '')}"
    }
    
    for zh_col, en_col in COLUMN_MAPPING.items():
        if zh_col in row:
            data[en_col] = bool(row[zh_col])
    
    data['affiliationTW'] = row.get('學校單位', '')
    data['correspondingTW'] = row.get('姓名', '')
    
    return data

def insert_into_postgres(data):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO publications (
        DOI, title, firstAuthor, authors, publisher, journal,
        published_year, abstract, URL, affiliationTW, correspondingTW,
        OR1, OR2, OR3, OR5, NOR1, NOR2, NOR3, LEGEND, MIC1, MIC2, MIC3, ODB
    ) VALUES %s
    ON CONFLICT (DOI) DO UPDATE SET
        title = EXCLUDED.title,
        firstAuthor = EXCLUDED.firstAuthor,
        authors = EXCLUDED.authors,
        publisher = EXCLUDED.publisher,
        journal = EXCLUDED.journal,
        published_year = EXCLUDED.published_year,
        abstract = EXCLUDED.abstract,
        URL = EXCLUDED.URL,
        affiliationTW = EXCLUDED.affiliationTW,
        correspondingTW = EXCLUDED.correspondingTW
    """
    
    values = [(
        d['DOI'], d['title'], d['firstAuthor'], d['authors'], d['publisher'], d['journal'],
        d['published_year'], d['abstract'], d['URL'], d['affiliationTW'], d['correspondingTW'],
        d.get('OR1', False), d.get('OR2', False), d.get('OR3', False), d.get('OR5', False),
        d.get('NOR1', False), d.get('NOR2', False), d.get('NOR3', False), d.get('LEGEND', False),
        d.get('MIC1', False), d.get('MIC2', False), d.get('MIC3', False), d.get('ODB', False)
    ) for d in data]

    execute_values(cursor, insert_query, values)
    conn.commit()
    cursor.close()
    conn.close()

def process_csv(file_path):
    df = pd.read_csv(file_path)
    processed_data = []

    for _, row in df.iterrows():
        paper_title = row['論文']
        crossref_data = fetch_crossref_info(paper_title)
        if crossref_data:
            processed_data.append(transform_data(row, crossref_data))

    insert_into_postgres(processed_data)

if __name__ == "__main__":
    process_csv('papers.csv')
