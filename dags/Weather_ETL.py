from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
import requests

@dag(
    dag_id="Weather_ETL",
    schedule=CronDataIntervalTimetable("0 18 * * *", timezone="Asia/Karachi"),
    start_date=datetime(2026, 5, 5),
    end_date=datetime(2026, 6, 30),
    catchup=True,
)
def Weather_ETL():

    @task
    def extract_data_api():
        api_key = Variable.get("OPENWEATHERMAP_API_KEY")
        city_name = "Lahore"
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric"

        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task
    def transform_data(data, **kwargs):
        if not data:
            raise ValueError("No data received!")
        
        return {
            'city': data['name'],
            'min_temp': data['main']['temp_min'],
            'max_temp': data['main']['temp_max'],
            'humidity': data['main']['humidity'],
            'execution_date': kwargs['ds']
        }

    @task
    def load_data(transformed):
        mysql_hook = MySqlHook(mysql_conn_id='mysql_local')
        
        sql = """
        INSERT INTO weather_forecast (city, min_temp, max_temp, humidity, execution_date)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            min_temp = VALUES(min_temp),
            max_temp = VALUES(max_temp),
            humidity = VALUES(humidity);
        """
        
        mysql_hook.run(sql, parameters=(
            transformed['city'],
            transformed['min_temp'],
            transformed['max_temp'],
            transformed['humidity'],
            transformed['execution_date']
        ))

    extracted = extract_data_api()
    transformed = transform_data(extracted)
    load_data(transformed)

Weather_ETL_dag = Weather_ETL()