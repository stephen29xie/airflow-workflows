# airflow-workflows
Apache Airflow workflows

> Airflow is a framework to programmatically author, schedule and monitor workflows.


# TO DO
* Move Connection setup to its own DAG
* SLAs
* email notifications on task fails and SLA misses
* Investigate Reddit functionality (should I be getting data from top, hot, new, etc.?). And also look into downvotes.
* Change executor from SequentialExecutor to LocalExecutor + non sqlite db