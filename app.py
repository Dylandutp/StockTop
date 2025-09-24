from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import undetected_chromedriver as uc
import os
import requests
import time
import re
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import IntegrityError
from datetime import datetime


DB_STRING = f"postgresql://{user}:{password}@{server}:{port}/{database}"

AWAIT_TIME = 60

def remove_repeat_string(s):
    return re.sub(r"^(.+)\1$", r"\1", s)

def setup_driver():
    # Set up Selenium WebDriver with options
    options = Options()
    # options.add_argument("--headless")
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors")
    options.add_argument('--disable-blink-features=AutomationControlled')
    driver = uc.Chrome(use_subprocess=True, options=options)
    driver.header_overrides = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/"
    }
    return driver

def init_db():
    engine = create_engine(DB_STRING, echo=False)
    try:
        inspector = inspect(engine)
        # Check if the table 'shareholding' exists, if not create it
        if not inspector.has_table('shareholding'):
            with engine.connect() as conn:
                conn.execute(text("""
                    CREATE TABLE shareholding (
                        uid uuid DEFAULT gen_random_uuid(),
                        company_id VARCHAR(10) NOT NULL,
                        ranking INT NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        shareholding BIGINT NOT NULL,
                        shareholding_ratio FLOAT NOT NULL,
                        pledge BIGINT NOT NULL,
                        pledge_ratio FLOAT NOT NULL,
                        date TIMESTAMP NOT NULL,
                        PRIMARY KEY (company_id, name)
                    )
                """))
                conn.execute(text("CREATE INDEX idx_company_id ON shareholding (company_id)"))
                conn.execute(text("CREATE UNLOGGED TABLE IF NOT EXISTS stg_shareholding AS TABLE shareholding WITH NO DATA"))
                conn.commit()
                print("Table 'shareholding' created successfully.")
            
    except Exception as e:
        raise Exception(f"Error creating table: {e}")

def get_id_from_db():
    engine = create_engine(DB_STRING, echo=False)
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 公司代號 FROM \"t187ap03_LO\""))
            return result.fetchall()  # List of tuples ex: (('8349',), ('2330',), ...)
    except Exception as e:
        raise Exception(f"Error querying data: {e}")

def insert_db(company_id, data):
    engine = create_engine(DB_STRING, echo=False)
    # Prepare parameterized SQL and data
    today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rk = 1
    sh = 0
    values = []
    for idx, row in enumerate(data, start=1):
        try:
            name = remove_repeat_string(row[1])
            shareholding = int(row[2].replace(",", ""))
            shareholding_ratio = float(row[3].replace("%", ""))
            pledge = int(row[4].replace(",", ""))
            pledge_ratio = float(row[5].replace("%", ""))
            if shareholding == sh:
                ranking = rk
            else:
                ranking = idx
                rk = ranking
                sh = shareholding
            values.append({
                'company_id': company_id,
                'ranking': ranking,
                'name': name,
                'shareholding': shareholding,
                'shareholding_ratio': shareholding_ratio,
                'pledge': pledge,
                'pledge_ratio': pledge_ratio,
                'date': today
            })
        except Exception as e:
            print(f"Error processing row for ID {company_id}: {e}")

    print(f"Prepared {len(values)} rows for insertion.")

    # Insert data into the database using bulk insert
    with engine.begin() as conn:
        try:
            conn.execute(text("DELETE FROM stg_shareholding WHERE company_id = :company_id"), {'company_id': company_id})
            insert_stmt = text("""
                INSERT INTO stg_shareholding (company_id, ranking, name, shareholding, shareholding_ratio, pledge, pledge_ratio, date)
                VALUES (:company_id, :ranking, :name, :shareholding, :shareholding_ratio, :pledge, :pledge_ratio, :date)
            """)
            conn.execute(insert_stmt, values)  # Bulk insert using list of dicts

            conn.execute(text("""
                SELECT pg_advisory_xact_lock(:company_id);

                WITH
                cur AS (
                    SELECT MAX("date") AS cur_date
                    FROM shareholding
                    WHERE company_id = :company_id
                ),
                nw AS (
                    SELECT MAX("date") AS new_date
                    FROM stg_shareholding
                    WHERE company_id = :company_id
                ),
                gate AS (
                    SELECT
                        (cur.cur_date IS NULL) OR (nw.new_date >= cur.cur_date + INTERVAL '1 month') AS allow
                    FROM cur, nw
                ),
                del AS (
                    DELETE FROM shareholding t
                    USING gate
                    WHERE gate.allow
                        AND t.company_id = :company_id
                    RETURNING 1
                )
                INSERT INTO shareholding (
                    company_id, ranking, name, shareholding, shareholding_ratio,
                    pledge, pledge_ratio, "date"
                )
                SELECT
                    s.company_id, s.ranking, s.name, s.shareholding,
                    s.shareholding_ratio, s.pledge, s.pledge_ratio, s."date"
                FROM stg_shareholding s
                JOIN gate ON gate.allow
                WHERE s.company_id = :company_id;
            """), {'company_id': company_id})
        except IntegrityError:
            print(f"IntegrityError while inserting")
        except Exception as e:
            raise Exception(f"Error inserting data: {e}")


if __name__ == '__main__':
    try:
        init_db()
        fail_list = []
        if os.path.exists("unfinished_list.txt"):
            with open("unfinished_list.txt", "r") as f:
                unfinished_ids = [line.strip() for line in f.readlines() if line.strip()]
            id_list = [(uid,) for uid in unfinished_ids]
            print(f"Resuming from unfinished list with {len(id_list)} IDs.")
        else:
            id_list = get_id_from_db()
            print(f"Total IDs to process: {len(id_list)}")
        with setup_driver() as driver:
            for idx, id_tuple in enumerate(id_list, start=1):
                id = id_tuple[0]
                url = f"https://www.iqvalue.com/Frontend/stock/shareholding?stockId={id}"
                print(f"Processing ID: {id}")
                driver.get(url)
                try:
                    print("Waiting for table to load...")
                    table = WebDriverWait(driver, 40).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, f"body > div.pagewp > div.mainwp.clfix > main > table"))
                    )
                except Exception as e:
                    print(f"Failed to load table for ID {id}: {e}")
                    fail_list.append((id,))
                    continue
                data = []
                print("Getting data from table...")
                for item in table.find_elements(By.XPATH, "./tbody/tr"):
                    if item.find_elements(By.TAG_NAME, "th"):
                        continue
                    row = [td.text for td in item.find_elements(By.TAG_NAME, "td")]
                    data.append(row)

                if not data:
                    print(f"No data found for ID {id}")
                    fail_list.append(id)
                    continue

                length = len(data)
                for i, d in enumerate(data[::-1]):
                    if d[0] == "大股東":
                        length = len(data) - i
                        break
                data = data[:length]
                print(f"Successfully retrieved {len(data)} rows for ID {id}")
                # Process and insert data into the database
                insert_db(id, data)
                print(f"Successfully Inserted data for ID {id}")
                time.sleep(AWAIT_TIME)  # Wait before processing the next ID
        os.remove("unfinished_list.txt") if os.path.exists("unfinished_list.txt") else None
        print("All IDs processed successfully.")
    except KeyboardInterrupt:
        unfinished_list = fail_list + id_list[idx:]
        with open("unfinished_list.txt", "w") as f:
            for item in unfinished_list:
                f.write(f"{item[0]}\n")
        print("Process interrupted by user.")
    except Exception as e:
        unfinished_list = fail_list + id_list[idx:]
        with open("unfinished_list.txt", "w") as f:
            for item in unfinished_list:
                f.write(f"{item[0]}\n")
        print("An error occurred:", str(e))