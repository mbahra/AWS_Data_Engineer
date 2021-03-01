import boto3
import urllib.parse
import uuid

s3_client = boto3.client('sagemaker')


def lambda_handler(event, context):

    dataLakeBucketName = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    uri = 's3://{}/{}'.format(dataLakeBucketName, key)
    transformJobName = 'xGoals-' + str(uuid.uuid4())
    S3OutputPath = 's3://{}/processed-data/api-football/sagemaker-autopilot/xGoals-predictions/'.format(dataLakeBucketName)
    modelName = 'XXX'  # Replace 'XXX' by your model name

    # xGoals batch transform job
    s3_client.create_transform_job(
        TransformJobName=transformJobName,
        ModelName=modelName,
        ModelClientConfig={
            'InvocationsTimeoutInSeconds': 200,
            'InvocationsMaxRetries': 1
        },
        MaxPayloadInMB=6,
        BatchStrategy='MultiRecord',
        TransformInput={
            'DataSource': {
                'S3DataSource': {
                    'S3DataType': 'S3Prefix',
                    'S3Uri': uri
                }
            },
            'ContentType': 'text/csv',
            'CompressionType': 'None',
            'SplitType': 'Line'
        },
        TransformOutput={
            'S3OutputPath': S3OutputPath,
            'Accept': 'text/csv',
            'AssembleWith': 'Line',
        },
        TransformResources={
            'InstanceType': 'ml.m5.large',
            'InstanceCount': 1,
        },
        DataProcessing={
            'InputFilter': "$[3:]",
            'OutputFilter': "$[0,1,-1]",
            'JoinSource': 'Input'
        },
    )

    print('Job', transformJobName, 'performed successfully!')
