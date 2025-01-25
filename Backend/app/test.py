import sqlite3

conn = sqlite3.connect("../movies.db")
query = "SELECT movieId, title, genres FROM movies LIMIT 10"
for row in conn.execute(query):
    print(row)
conn.close()
