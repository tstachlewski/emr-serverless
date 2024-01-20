import os
import sys
import boto3

bucket = "tomke-demo-emr"
key = "data/people-input/people-02.csv"

index = key.rfind("/") + 1

file = key[key.rfind("/") + 1:]

print(index)
print(file)