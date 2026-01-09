import sqlite3

DB_NAME = "gold.db"

def criar_database():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    with open('gold/schema.sql', 'r', encoding='utf-8') as f:
        schema = f.read()
    
    try:
        cursor.executescript(schema)
        conn.commit()
        print("Banco de dados criado com sucesso")
    except sqlite3.Error as e:
        print(f"Erro ao criar banco de dados: {e}")

if __name__ == "__main__":
    criar_database()