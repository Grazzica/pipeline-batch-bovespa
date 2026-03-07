import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')

    response = glue.start_job_run(JobName='tc2-etl-job')

    return {
        'statusCode': 200,
        'body': f"Glue job started: {response['JobRunID']}"
    }