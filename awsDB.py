import sys
sys.path.append('.aws')
import config
import boto3
import csv

# Connect to AWS
s3 = boto3.resource(
    's3', 
    aws_access_key_id = config.aws_access_key_id,
    aws_secret_access_key = config.aws_secret_access_key
)

# Make a bucket
bucket_name = 'gamer-bucket-1337-noob'
try:
    s3.create_bucket(
        Bucket = bucket_name,
        CreateBucketConfiguration = {'LocationConstraint': config.region}
    )
except:
    print("Bucket creation error... (likely already exists)")
    #traceback.print_exc()
# Now we have the bucket
bucket = s3.Bucket(bucket_name)
# Make bucket readable
bucket.Acl().put(ACL='public-read')

"""Print bucket names
for b in s3.buckets.all():
    print(b.name)
"""

# get our DBMS
dyndb = boto3.resource(
    'dynamodb',
    region_name = config.region,
    aws_access_key_id = config.aws_access_key_id,
    aws_secret_access_key = config.aws_secret_access_key
)

# Make a table
table_name = 'TestDataTbl'
try:
    table = dyndb.create_table(
        TableName = table_name,
        KeySchema = [
            {'AttributeName': 'PartitionKey', 'KeyType': 'HASH'},
            {'AttributeName': 'RowKey', 'KeyType': 'RANGE'}
        ],
        AttributeDefinitions=[
            {'AttributeName': 'PartitionKey', 'AttributeType': 'S'},
            {'AttributeName': 'RowKey', 'AttributeType': 'S'},
        ],
        ProvisionedThroughput = {
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except:
    print("Table creation error... (likely already exists)")
    #traceback.print_exc()
# Now we have the table
table = dyndb.Table(table_name)
# We have to wait on the table to actually be created though
table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

print()
# Now to populate the table
with open('data/experiments.csv', 'r', encoding='utf-8') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    header = True
    for row in csvf:
        # ignore the header row (no data!)
        if header:
            header = False
            continue

        # Get the experiment data...
        exp_name = row[4]
        exp_body = open('data/'+exp_name, 'rb')
        # Put it (row data, not parsed) in the bucket publicly...
        s3.Object(bucket_name, exp_name).put(Body=exp_body)
        s3.Object(bucket_name, exp_name).Acl().put(ACL='public-read')
        # Build its url...
        url = "https://s3-"+config.region+".amazonaws.com/"+bucket_name+"/"+exp_name
        metadata_item = {
            'PartitionKey': row[0],
            'RowKey': row[1],
            'description': row[3],
            'date': row[2],
            'url': url
        }
        try:
            table.put_item(Item=metadata_item)
        except:
            print("Table put failure... (may already be in table)")
            #traceback.print_exc()

# Cool, we updated the DB, now let's execute our query
if len(sys.argv) < 3:
    print("Not enough arguments for your query...\nThe dynamoDB primary key should follow your command.\nExample:\tpython awsDB.py experiment1 data1\n")
    sys.exit(1)

try:
    response = table.get_item(Key={'PartitionKey': sys.argv[1], 'RowKey': sys.argv[2]})
    item = response['Item']
except:
    print("No entry found for primary key {'PartitionKey': "+sys.argv[1]+", 'RowKey': "+sys.argv[2]+"}\n")
    sys.exit(1)

print("Entry found!")
print(item)
print()