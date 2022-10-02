
from airflow import DAG
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator



args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
    'retries': 0
    # 'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='airflowTweetsProj',
    default_args=args,
    schedule_interval=None,
    tags=['tweeter']
)

createDependencies = BashOperator(
    task_id='createDependencies',
    # bash_command='python /tmp/pycharm_project_387/createDependencies.py',
    bash_command='python /tmp/pycharm_project_149/createDependencies.py',
    dag=dag
)
#
getTweets = BashOperator(
    task_id='getTweets',
    bash_command='python /tmp/pycharm_project_149/kafkaProducerTwitterAPI.py',
    dag=dag,
)


saveToHDFS = BashOperator(
    task_id='saveToHDFS',
    bash_command='python /tmp/pycharm_project_149/sparkConsumerTwitterToHDFS.py',
    dag=dag,
)

saveToMySql = BashOperator(
    task_id='saveToMySql',
    bash_command='python /tmp/pycharm_project_149/sparkConsumerTwitterToMySQL.py',
    dag=dag
)

saveFromHiveTablesToGCP = BashOperator(
    task_id='saveFromHiveTablesToGCP',
    bash_command='python /tmp/pycharm_project_149/buildHiveTablesAndSaveToGCP.py',
    dag=dag
)

updateCategory = BashOperator(
    task_id='updateCategory',
    bash_command='python /tmp/pycharm_project_149/saveFromGsheetsToMySQL.py',
    dag=dag
)
#

# createDependencies >> saveFromHiveTablesToGCP >> updateCategory

#
createDependencies >> getTweets
createDependencies >> [saveToHDFS, saveToMySql]
saveToHDFS >> saveFromHiveTablesToGCP
saveToMySql >> updateCategory



if __name__ == "__main__":
    dag.cli()

# # [START invoke kafka producer]
# def get_tweets():
#     print("start get data from twitter API")
#     kafkaProducer.connectToTwitter()
#
#
#
# getData = PythonOperator(
#     task_id='exec connectToTweeter',
#     provide_context=True,
#     python_callable=get_tweets,
#     # schedule_interval="0 0 0/1 1/1 * ? *",
#     dag=dag
# )
# # [END invoke kafka producer]
#
#
# # [START invoke HDFS consumer]
# def saveToHDFS():
#     print("start save to hdfs")
#     hdfsConsumer.saveDataToHDFS()
#
#
#
# saveHDFS = PythonOperator(
#     task_id='exec saveToHDFS',
#     provide_context=True,
#     python_callable=mySQLConsumer.saveToMySQL(),
#     dag=dag
# )
# # [END invoke HDFS consumer]
#
# # [START invoke mySQL consumer]
# def saveToMySQL():
#     print("start save to mySql")
#     hdfsConsumer.saveDataToHDFS()
#
#
#
# saveMySQL = PythonOperator(
#     task_id='exec saveToMySQL',
#     provide_context=True,
#     python_callable=saveToMySQL,
#     dag=dag
# )
# # [END invoke HDFS consumer]
#
#
# getData >> [saveHDFS, saveMySQL]
#
