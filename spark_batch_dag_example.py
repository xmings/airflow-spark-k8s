from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from .spark_k8s import SparkOnK8s
from .dag_utils import OperatorFactory, OperatorLink

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
}

spark_on_k8s = SparkOnK8s(jar_path="spark-merge-1.0-SNAPSHOT-jar-with-dependencies.jar")

dag = DAG(
    dag_id='spark-batch',
    default_args=default_args,
    description='submit spark-batch on kubernetes',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)
op_factory = OperatorFactory(dag=dag)

main_link = OperatorLink(op_factory=op_factory)\
    .to_dummy(name="Start")\
    .to_py(func=lambda: spark_on_k8s.run_spark_on_k8s(main_class="FirstClass"), name="FirstClass")\
    .to_py(func=lambda: spark_on_k8s.run_spark_on_k8s(main_class="SecondClass"), name="SecondClass")\
    .to_py(func=lambda: spark_on_k8s.run_spark_on_k8s(main_class="ThirdClass"), name="ThirdClass")\
    .to_dummy(name="Finished")


if __name__ == "__main__":
    dag.cli()

