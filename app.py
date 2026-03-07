from flask import Flask, jsonify, render_template
import psycopg2
import os
import pg8000.native
from flask import render_template


app = Flask(__name__)

def get_school_db():
    return pg8000.native.Connection(
        host=os.environ["SCHOOL_DB_HOST"],
        port=int(os.environ.get("SCHOOL_DB_PORT", 5432)),
        database=os.environ.get("SCHOOL_DB_NAME", "maple_school"),
        user=os.environ["SCHOOL_DB_USER"],
        password=os.environ["SCHOOL_DB_PASSWORD"],
        ssl_context=True
    )

def query(conn, sql):
    """Run a query and return list of dicts."""
    rows = conn.run(sql)
    cols = [c["name"] for c in conn.columns]
    return [dict(zip(cols, row)) for row in rows]

def safe_float(val):
    """Convert Decimal/None to float safely."""
    if val is None:
        return 0.0
    try:
        return float(val)
    except:
        return 0.0

def clean_row(row):
    """Convert all Decimal/date values to JSON-serializable types."""
    result = {}
    for k, v in row.items():
        if hasattr(v, '__float__'):          # Decimal
            result[k] = float(v)
        elif hasattr(v, 'isoformat'):        # date/datetime
            result[k] = v.isoformat()
        else:
            result[k] = v
    return result


@app.route("/school-dashboard")
def school_dashboard():
    conn = None
    try:
        conn = get_school_db()

        annual_kpis     = [clean_row(r) for r in query(conn, "SELECT * FROM vw_annual_kpis ORDER BY year")]
        monthly_pnl     = [clean_row(r) for r in query(conn, "SELECT * FROM vw_monthly_pnl ORDER BY year, month")]
        fee_by_grade    = [clean_row(r) for r in query(conn, "SELECT * FROM vw_fee_by_grade ORDER BY year, grade")]
        outstanding     = [clean_row(r) for r in query(conn, "SELECT * FROM vw_outstanding_fees LIMIT 50")]
        staff           = [clean_row(r) for r in query(conn, "SELECT * FROM staff ORDER BY job_code, last_name")]
        staff_cost      = [clean_row(r) for r in query(conn, "SELECT * FROM vw_staff_cost_summary ORDER BY est_monthly_total_usd DESC")]
        materials_by_cat= [clean_row(r) for r in query(conn, "SELECT * FROM vw_materials_by_category ORDER BY year, total_spend_usd DESC")]
        payroll_monthly = [clean_row(r) for r in query(conn, "SELECT * FROM vw_monthly_payroll ORDER BY year, month")]

        return render_template(
            "school_dashboard.html",
            annual_kpis     = annual_kpis,
            monthly_pnl     = monthly_pnl,
            fee_by_grade    = fee_by_grade,
            outstanding     = outstanding,
            staff           = staff,
            staff_cost      = staff_cost,
            materials_by_cat= materials_by_cat,
            payroll_monthly = payroll_monthly,
        )

    except Exception as e:
        return f"<h2>Dashboard Error</h2><pre>{str(e)}</pre>", 500
    finally:
        if conn:
            conn.close()
            

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



