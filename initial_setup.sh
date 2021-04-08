#!/bin/bash
# delete aws
sudo rm -rf /opt/conda/aws
sudo rm /opt/conda/bin/aws

#install aws cli v2
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.0.30.zip" -o "awscliv2.zip"
unzip awscliv2.zip
./aws/install -i /opt/conda/aws -b /opt/conda/bin

# update boto3
pip install boto3==1.17.45

# set ENV VARs for aws cli
export AWS_ACCESS_KEY_ID=AKIA3V3HAYPXCLJTCEQZ
export AWS_SECRET_ACCESS_KEY=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
export AWS_DEFAULT_REGION=us-east-1

#pre-launch airflow
/opt/airflow/start.sh