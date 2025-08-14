"""Template for newly generated pipelines."""

from openhexa.sdk import current_run, pipeline


@pipeline("dhis2_data_extraction_lionel")
def dhis2_data_extraction_lionel():
    """Write your pipeline orchestration here.

    Pipeline functions should only call tasks and should never perform IO operations or expensive computations.
    """
    count = task_1()
    task_2(count)
    print("Pipeline execution completed - all tasks executed successfully.")


@dhis2_data_extraction_lionel.task
def task_1():
    """Put some data processing code here."""
    current_run.log_info("In task 1...")

    return 42


@dhis2_data_extraction_lionel.task
def task_2(count):
    """Put some data processing code here."""
    current_run.log_info(f"In task 2... count is {count}")


if __name__ == "__main__":
    dhis2_data_extraction_lionel()
