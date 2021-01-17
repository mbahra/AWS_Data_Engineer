import boto3
import uuid

def createBucketName(bucketPrefix):
    # The generated bucket name must be between 3 and 63 chars long
    # A UUID4’s string representation is 36 characters long
    # bucket_prefix must be less than 27 chars long
    return ''.join([bucketPrefix, str(uuid.uuid4())])

def createBucket(bucketPrefix, s3Connection):
    session = boto3.session.Session()
    currentRegion = session.region_name
    bucketName = createBucketName(bucketPrefix)
    bucketResponse = s3Connection.create_bucket(
        Bucket=bucketName,
        CreateBucketConfiguration={
        'LocationConstraint': currentRegion})
    print("S3 bucket created:", bucketName, currentRegion)
    return bucketResponse, bucketName

def main():

    s3_client = boto3.client('s3')

    rawDataBucket, rawDataBucketName = createBucket("raw-data-", s3_client)
    processedDataBucket, processedDataBucket = createBucket("processed-data-", s3_client)

    s3_client.upload_file('teamcodes.csv', rawDataBucketName, 'teamcodes.csv')
    print("teamcodes.csv uploaded into", rawDataBucketName)


if __name__ == '__main__':
    main()
