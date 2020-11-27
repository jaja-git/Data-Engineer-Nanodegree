import pandas as pd
import boto3
import json
import configparser
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                    = config.get('AWS','KEY')
SECRET                 = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

CLUSTER_IDENTIFIER     = config.get("DWH","CLUSTER_IDENTIFIER")
DB_NAME                = config.get("DWH","DB_NAME")
DB_USER                = config.get("DWH","DB_USER")
DB_PASSWORD            = config.get("DWH","DB_PASSWORD")
DB_PORT                = config.get("DWH","DB_PORT")

IAM_ROLE_NAME          = config.get("DWH", "IAM_ROLE_NAME")


ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

s3 = boto3.resource('s3',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )

iam = boto3.client('iam',
                      region_name='us-west-2',
                      aws_access_key_id=KEY,
                      aws_secret_access_key=SECRET
                    )

redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )



# 1.1 Create the role 
try:
    print("Creating a new IAM Role...") 
    dwhRole = iam.create_role(
        Path='/',
        RoleName=IAM_ROLE_NAME,
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)

# 1.2 Attach Policy    
print("Attaching Policy...")
try:
    iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']
except Exception as e:
    print(e)

# 1.2 Get the IAM role ARN
print("IAM role ARN:")
roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']
print(roleArn)

# 2 Create the cluster
try:
    response = redshift.create_cluster(        
        #HW
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DB_NAME,
        ClusterIdentifier=CLUSTER_IDENTIFIER,
        MasterUsername=DB_USER,
        MasterUserPassword=DB_PASSWORD,
        
        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )
except Exception as e:
    print(e)

# Display properties

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

myClusterProps = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]

prettyRedshiftProps(myClusterProps)
