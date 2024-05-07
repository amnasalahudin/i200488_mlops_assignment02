from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_article_details(url):
    """Extracts title and description from an article URL."""
    try:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        title_element = soup.find('title')
        title = title_element.text.strip() if title_element else None
        
        paragraphs = soup.find_all('p')
        description = ' '.join([p.text.strip() for p in paragraphs if p.text.strip()]) if paragraphs else None
        
        if not title or not description:
            logging.warning(f"No valid title or description for URL: {url}")
            return None, None
        return title, description
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for {url}: {str(e)}")
        return None, None
    except Exception as e:
        logging.error(f"Error processing {url}: {str(e)}")
        return None, None

def extract():
    """Extracts article links from BBC.com and Dawn.com, then fetches title and description from those articles."""
    data = []
    sources = {
        'BBC': 'https://www.bbc.com',
        'Dawn': 'https://www.dawn.com'
    }
    selectors = {
        'BBC': 'a[data-testid="internal-link"]',  # BBC links within cards
        'Dawn': 'article.story a.story__link'  # Dawn story links
    }

    for source, base_url in sources.items():
        response = requests.get(base_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        links = [a['href'] for a in soup.select(selectors[source]) if 'href' in a.attrs]
        links = [link if link.startswith('http') else base_url + link for link in links]

        for link in links:
            title, description = extract_article_details(link)
            if title and description:
                data.append({'title': title, 'description': description, 'source': source})
            else:
                logging.info(f"No data extracted for {link}")
    return data

def transform(data):
    """Transforms data for loading"""
    transformed_data = [{'id': i+1, 'title': row['title'], 'description': row['description'], 'source': row['source']} for i, row in enumerate(data)]
    return transformed_data

def load(data):
    """Loads data into a CSV file"""
    df = pd.DataFrame(data)
    df = df.dropna(subset=['title', 'description'])  # Drop rows where 'title' or 'description' is None
    df.to_csv('/mnt/c/Users/Spectre/OneDrive/Desktop/i200488_mlops_assignment02/extracted_data.csv', index=False) 

# Define the DAG
dag = DAG(
    dag_id='web_article_scraping',
    start_date=datetime(2024, 5, 7),
    schedule_interval='@daily'
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=lambda: transform(extract()),
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=lambda: load(transform(extract())),
    dag=dag
)

# Bash command to add and push data to DVC
dvc_add_and_push_command = """
cd /mnt/c/Users/Spectre/OneDrive/Desktop/i200488_mlops_assignment02 && \
dvc add extracted_data.csv && \
git add . && \
git commit -m 'committed via airflow' && \
dvc push
git push
"""

dvc_task = BashOperator(
    task_id='dvc_add_and_push',
    bash_command=dvc_add_and_push_command,
    dag=dag
)

# Set task dependencies
extract_task >> transform_task >> load_task >> dvc_task
