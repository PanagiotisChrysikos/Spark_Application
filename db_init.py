import sqlite3

db_path = '/your/path/to/data.db'

def initialize_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS subscribers (
            row_key TEXT PRIMARY KEY,
            sub_id TEXT,
            act_dt TEXT
        )
    ''')
    conn.commit()
    conn.close()

if __name__ == '__main__':
    initialize_db()
    print("Database initialized successfully!")

