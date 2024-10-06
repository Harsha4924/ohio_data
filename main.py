import pandas as pd
import sqlite3
from flask import Flask, request, jsonify

file_path = r'data.xls'
df = pd.read_excel(file_path)

annual_data = df.groupby('API WELL  NUMBER').agg({
    'OIL': 'sum',
    'GAS': 'sum',
    'BRINE': 'sum'
}).reset_index()

# output_file_path = r'annual_production_data.xlsx'
# annual_data.to_excel(output_file_path, index=False)

# print(annual_data)

conn = sqlite3.connect('production_data.db')  
cursor = conn.cursor()

cursor.execute('DROP TABLE IF EXISTS production_data')

cursor.execute('''
CREATE TABLE IF NOT EXISTS production_data (
    api_well_number TEXT PRIMARY KEY,
    oil INTEGER,
    gas INTEGER,
    brine INTEGER
)
''')

for _, row in annual_data.iterrows():
    cursor.execute('''
    INSERT OR REPLACE INTO production_data (api_well_number, oil, gas, brine)
    VALUES (?, ?, ?, ?)
    ''', (str(row['API WELL  NUMBER']), int(row['OIL']), int(row['GAS']), int(row['BRINE'])))

conn.commit()
conn.close()

print("data successfully inserted into database.")

app = Flask(__name__)

def get_db_connection():
    conn = sqlite3.connect('production_data.db')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/data', methods=['GET'])
def get_annual_data():
    well = request.args.get('well')  
    # print(well)
    if well:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM production_data WHERE api_well_number = ?', (well,))
        row = cursor.fetchone()
        # print("row",dict(row))
        conn.close()

        if row:
            return jsonify({
                "oil": row["oil"],
                "gas": row["gas"],
                "brine": row["brine"]
            })
        else:
            return jsonify({"error": "Well not found"}), 404
    else:
        return jsonify({"error": "Well number is required"}), 400



if __name__ == '__main__':
    app.run(port=8080)
