from airflow.sdk import dag, task, Variable
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.mysql.hooks.mysql import MySqlHook
import requests
from datetime import datetime as dt

@dag(
    dag_id="Weather_ETL",
    schedule=CronDataIntervalTimetable("0 22 * * *", timezone="Asia/Karachi"),
    start_date=datetime(2026, 5, 5),
    end_date=datetime(2026, 6, 30),
    catchup=True,
)
def Weather_ETL():

    @task.python
    def extract_data_api():
        api_key = Variable.get("OPENWEATHER_API_KEY")
        city_name = "Lahore"
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    @task.python
    def transform_data(data, logical_date=None):
        if data is None:
            raise ValueError("No data received!")

        execution_date = str(logical_date.date()) if logical_date else ''

        return {
            'city': data['name'],
            'min_temp': data['main']['temp_min'],
            'max_temp': data['main']['temp_max'],
            'humidity': data['main']['humidity'],
            'recorded_at': dt.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            'execution_date': execution_date
        }

    @task.python
    def load_data(transformed):
        if transformed is None:
            raise ValueError("No data to load!")

        mysql_hook = MySqlHook(mysql_conn_id='mysql_local')

        sql = """
        INSERT INTO   etl_weather_pipline_data.weather_forecast (city, min_temp, max_temp, humidity, recorded_at, execution_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            min_temp = VALUES(min_temp),
            max_temp = VALUES(max_temp),
            humidity = VALUES(humidity),
            recorded_at = VALUES(recorded_at);
        """

        mysql_hook.run(sql, parameters=(
            transformed['city'],
            transformed['min_temp'],
            transformed['max_temp'],
            transformed['humidity'],
            transformed['recorded_at'],
            transformed['execution_date']
        ))

        print(f"Data loaded for {transformed['city']} at {transformed['recorded_at']}!")

    extracted = extract_data_api()
    transformed = transform_data(extracted)
    load_data(transformed)

Weather_ETL_dag = Weather_ETL()