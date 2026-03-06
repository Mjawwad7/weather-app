from flask import Flask, jsonify, render_template
import psycopg2
import os

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        host=os.environ.get('DB_HOST'),
        database=os.environ.get('DB_NAME'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD'),
        port=5432
    )
    return conn

@app.route('/')
def home():
    return render_template('landing.html')

@app.route('/dashboard')
def dashboard():
    return render_template('index.html')

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


