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

    @task.python
    def extract_data_api():
        api_key = Variable.get("OPENWEATHERMAP_API_KEY")
        city_name = "Lahore"
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}&units=metric"

        try:

            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"API failed: {response.status_code}")
        except Exception as e:
            print(f"Error fetching data: {e}")
            return None    

    @task.python
    def transform_data(data, **kwargs):
        if data is None:
            raise ValueError("No data received!")
        
        execution_date = kwargs['ds']
        
        transformed= {
            'city': data['name'],
            'min_temp': data['main']['temp_min'],
            'max_temp': data['main']['temp_max'],
            'humidity': data['main']['humidity'],
            'execution_date': execution_date
        }
        return transformed  

    @task.python
    def load_data(transformed):
        if transformed is None:
            raise ValueError("No data to load!")
        
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
        
        print(f" Data loaded for {transformed['city']}!")

    extracted = extract_data_api()
    transformed = transform_data(extracted)
    load_data(transformed)

Weather_ETL_dag = Weather_ETL()