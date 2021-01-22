#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Instructions:
    1. Do not forget to add the --auto-terminate field because EMR clusters are costly. 
    Once you run this script, you’ll be given a unique cluster ID. 
    Check the status of your cluster using `aws emr --cluster-id <cluster_id>`.
    2. We'll be creating an EMR cluster for the exercise.
    3. First, install `awscli` using pip.  You can get instructions for MacOS, Windows, Linux here  on [AWS Documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html).
    4. This will give you access to create an EMR cluster and EC2 cluster. The EC2 cluster shows a status of all the clusters with your keys, etc. It does a ton of things!
    5. Once it's installed, run the script below to launch your cluster. Be sure to include the appropriate file names within the <> in the code.
"""i

# Add your cluster name
aws emr create-cluster 
--name "my-emr-cluster"
--use-default-roles  
--release-label emr-5.28.0 
--instance-count 3 
--applications Name=Spark  
--bootstrap-actions Path=s3://aws-emr-resources-926236161117-us-west-2/bootstrap_emr.sh
--ec2-attributes KeyName=spark-cluster
--instance-type m5.xlarge
--auto-terminate`

# Specify your cluster name 
YOUR_CLUSTER_NAME: "my-emr-cluster

# Insert your IAM KEYNAME - Remember, your IAM key name is saved under .ssh/ directory
YOUR_KEY_NAME: "spark-cluster"

# Specify your bootstrap file. Please note that this step is optional. It should be an executable (.sh file) in an accessible S3 location. If you aren't going to use the bootstrap file, you can remove the `--bootstrap-actions` tag above.
# This file is provided in the zipped folder titled “Exercise_Creating EMR Cluster” at the bottom of this page.

# In this EMR script, execute using Bootstrap
YOUR_BOOTSTRAP_FILENAME: "~/Documents/Github/Data-Engineer-Nanodegree/3-Data-Lakes-with-Spark/bootstrap_emr.sh"


Final_command = "aws emr create-cluster --name my-emr-cluster --use-default-roles --release-label emr-5.28.0 --instance-count 3 --applications Name=Spark  --bootstrap-actions Path=s3://aws-emr-resources-926236161117-us-west-2/bootstrap_emr.sh --ec2-attributes KeyName=spark-cluster --instance-type m5.xlarge --auto-terminate"