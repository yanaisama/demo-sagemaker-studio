from airflow.decorators import dag
from airflow.utils.dates import days_ago
from workflows.airflow.providers.amazon.aws.operators.sagemaker_workflows \
    import NotebookOperator

###############################################################################
#
# Enter in your desired schedule as WORKFLOW_SCHEDULE.  Some options include:
#
# '@daily' (daily at midnight)
# '@hourly' (every hour, at the top of the hour)
# '30 */3 * * *' (a CRON string, run at minute 30 past every 3rd hour)
# '0 8 * * 1-5' (a CRON string, run every weekday at 8am)
#
###############################################################################

WORKFLOW_SCHEDULE = None

###############################################################################
#
# Enter in the path to your notebook as NOTEBOOK_PATH. Example:
# 'src/workflows/dags/mynotebook.ipynb'
#
###############################################################################

NOTEBOOK_PATH = 'src/<path-to-notebook-file>'


default_args = {
    'owner': 'engenheiro-a',
}


@dag(
    dag_id='workflow-ta83qxy',
    default_args=default_args,
    schedule_interval=WORKFLOW_SCHEDULE,
    start_date=days_ago(2),
    is_paused_upon_creation=False,
    tags=['bu-a-dev-projeto', 'engenheiro-a'],
    catchup=False
)
def single_notebook():
    def initial_notebook_task():
        notebook1 = NotebookOperator(
               task_id="initial",
               input_config={'input_path': NOTEBOOK_PATH, 'input_params': {}},
               output_config={'output_formats': ['NOTEBOOK']},
               wait_for_completion=True,
               poll_interval=5
           )
        return notebook1
    initial_notebook_task()


single_notebook = single_notebook()
