from datetime import datetime

from include.utils.connections import add_gcp_connection
from include.utils.dataset import add_dataset, add_reference_tables
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG

from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig

@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['uniswap_v3_positions'],
)

def uniswap_v3_positions():
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    _add_gcp_connection = PythonOperator(
        task_id='add_gcp_connection',
        python_callable=add_gcp_connection,
        provide_context=True,
        trigger_rule='all_done',  
    )

    _add_dataset = add_dataset()
    
    _add_reference_tables = add_reference_tables()

    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        ),
    )

    begin >> _add_gcp_connection >> _add_dataset
    _add_dataset >> _add_reference_tables 
    _add_reference_tables >> transform >> end

uniswap_v3_positions()
