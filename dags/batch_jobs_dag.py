from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2023,11,20),
  'retries': 0
}

# schedule_interval='50 16 * * *'

with DAG('batch_layer_dag', default_args=default_args, catchup=False, schedule_interval='50 16 * * *') as dag:
  inventory_ingest = BashOperator(
    task_id="inventory_ingest",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_jobs/inventoryIngest.py"
  )

  products_ingest = BashOperator(
    task_id="products_ingest",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_jobs/productsIngest.py"
  )

  users_ingest = BashOperator(
    task_id="users_ingest",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_jobs/usersIngest.py"
  )

  user_detail_ingest = BashOperator(
    task_id="user_detail_ingest",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_jobs/userDtIngest.py"
  )

  orders_ingest = BashOperator(
    task_id="orders_ingest",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_jobs/ordersIngest.py"
  )

  order_detail_ingest = BashOperator(
    task_id="order_detail_ingest",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_jobs/orderDtIngest.py"
  )

  transform = BashOperator(
    task_id="transform",
    bash_command="cd /opt/airflow/src/ && python /opt/airflow/src/batch_jobs/transform.py"
  )

  end = EmptyOperator(task_id="done", trigger_rule='all_success')

  (inventory_ingest >> products_ingest >> users_ingest >> user_detail_ingest >>orders_ingest >> order_detail_ingest >> transform) >> end
