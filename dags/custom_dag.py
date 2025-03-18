from __future__ import annotations

import random
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="random_number_calculations",
    schedule=None,  
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    def generate_random_number():
        random_number = random.randint(1, 1000)
        return random_number

    def calculate_power_of_two(ti):
        random_number = ti.xcom_pull(task_ids="generate_random_number_task")
        power_of_two = 2**random_number
        return power_of_two

    def calculate_power_of_three(ti):
        random_number = ti.xcom_pull(task_ids="generate_random_number_task")
        power_of_three = 3 ** (random_number - 300)
        return power_of_three

    def calculate_difference(ti):
        power_of_two = ti.xcom_pull(task_ids="calculate_power_of_two_task")
        power_of_three = ti.xcom_pull(task_ids="calculate_power_of_three_task")
        difference = power_of_two - power_of_three
        print(f"Разница: {difference}")

    generate_random_number_task = PythonOperator(
        task_id="generate_random_number_task",
        python_callable=generate_random_number,
    )

    calculate_power_of_two_task = PythonOperator(
        task_id="calculate_power_of_two_task",
        python_callable=calculate_power_of_two,
    )

    calculate_power_of_three_task = PythonOperator(
        task_id="calculate_power_of_three_task",
        python_callable=calculate_power_of_three,
    )

    calculate_difference_task = PythonOperator(
        task_id="calculate_difference_task",
        python_callable=calculate_difference,
    )

    generate_random_number_task >> [calculate_power_of_two_task, calculate_power_of_three_task]
    [calculate_power_of_two_task, calculate_power_of_three_task] >> calculate_difference_task
