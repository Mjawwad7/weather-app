from flask import Flask, jsonify
import psycopg2
import os

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        host=os.environ.get('weather-db.ctwkc0uagqj2.us-east-2.rds.amazonaws.com'),
        database=os.environ.get('postgres'),
        user=os.environ.get('postgres'),
        password=os.environ.get('GAz6TXy6mPY3dAq'),
        port=5432
    )
    return conn

@app.route('/')
def home():
    return jsonify({"status": "Weather API is running"})

@app.route('/weather')
def get_weather():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT city, temperature_c, humidity_percent, 
               weather_condition, wind_speed_ms, timestamp
        FROM weather_data
        ORDER BY timestamp DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    data = []
    for row in rows:
        data.append({
            "city": row[0],
            "temperature_c": row[1],
            "humidity_percent": row[2],
            "weather_condition": row[3],
            "wind_speed_ms": row[4],
            "timestamp": str(row[5])
        })
    return jsonify(data)

@app.route('/weather/latest')
def get_latest():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ON (city) 
               city, temperature_c, humidity_percent,
               weather_condition, wind_speed_ms, timestamp
        FROM weather_data
        ORDER BY city, timestamp DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    data = []
    for row in rows:
        data.append({
            "city": row[0],
            "temperature_c": row[1],
            "humidity_percent": row[2],
            "weather_condition": row[3],
            "wind_speed_ms": row[4],
            "timestamp": str(row[5])
        })
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True)