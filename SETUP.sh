#!/usr/bin/env bash

# Change the variables to your environment
export BUCKET_NAME=<redacted>
export VPCID=<redacted>
export ROUTE_TABLE_ID=<redacted>
export SERVICE_ROLE_NAME=my-glue-service-role
export REPO_HOME=<redacted>/git/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/project/starter


# Make the bucket
aws s3 mb s3://$BUCKET_NAME


# Identify the virtual private cloud (set VPCID)
aws ec2 describe-vpcs


# Identify routing table (set ROUTE_TABLE_ID)
aws ec2 describe-route-tables


# Create an S3 Gateway endpoint
aws ec2 create-vpc-endpoint --vpc-id $VPCID --service-name com.amazonaws.us-west-2.s3 --route-table-ids $ROUTE_TABLE_ID


# Create the Glue service IAM role
aws iam create-role --role-name $SERVICE_ROLE_NAME --assume-role-policy-document '{
  "Version": "2012-10-17", 
  "Statement": [{"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}]
}'


# Grant Glue privileges on the new bucket
aws iam put-role-policy --role-name $SERVICE_ROLE_NAME \
  --policy-name S3Access \
  --policy-document '{ 
    "Version": "2012-10-17", 
    "Statement": [ 
      { 
        "Sid": "ListObjectsInBucket", 
        "Effect": "Allow", 
        "Action": [ "s3:ListBucket" ], 
        "Resource": [ "arn:aws:s3:::'${BUCKET_NAME}'" ] 
      }, 
      { 
        "Sid": "AllObjectActions", 
        "Effect": "Allow", 
        "Action": "s3:*Object", 
        "Resource": [ "arn:aws:s3:::'${BUCKET_NAME}'/*" ] 
      } 
      ] }'


# Grant general Glue privileges
aws iam put-role-policy --role-name $SERVICE_ROLE_NAME --policy-name GlueAccess \
  --policy-document '{
    "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:*",
        "s3:GetBucketLocation",
        "s3:ListBucket",
        "s3:ListAllMyBuckets",
        "s3:GetBucketAcl",
        "ec2:DescribeVpcEndpoints",
        "ec2:DescribeRouteTables",
        "ec2:CreateNetworkInterface",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeSubnets",
        "ec2:DescribeVpcAttribute",
        "iam:ListRolePolicies",
        "iam:GetRole",
        "iam:GetRolePolicy",
        "cloudwatch:PutMetricData"
      ],
      "Resource": [
        "*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:CreateBucket",
        "s3:PutBucketPublicAccessBlock"
      ],
      "Resource": [
        "arn:aws:s3:::aws-glue-*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::aws-glue-*/*",
        "arn:aws:s3:::*/*aws-glue-*/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::crawler-public*",
        "arn:aws:s3:::aws-glue-*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:AssociateKmsKey"
      ],
      "Resource": [
        "arn:aws:logs:*:*:/aws-glue/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "ec2:CreateTags",
        "ec2:DeleteTags"
      ],
      "Condition": {
        "ForAllValues:StringEquals": {
          "aws:TagKeys": [
            "aws-glue-service-resource"
          ]
        }
      },
      "Resource": [
        "arn:aws:ec2:*:*:network-interface/*",
        "arn:aws:ec2:*:*:security-group/*",
        "arn:aws:ec2:*:*:instance/*"
      ]
    }
  ]}'


# Copy data from the project starter repo to the s3 bucket
aws s3 cp $REPO_HOME/accelerometer/landing s3://$BUCKET_NAME/accelerometer/landing --recursive
aws s3 cp $REPO_HOME/customer/landing s3://$BUCKET_NAME/customer/landing --recursive
aws s3 cp $REPO_HOME/step_trainer/landing s3://$BUCKET_NAME/step_trainer/landing --recursive

