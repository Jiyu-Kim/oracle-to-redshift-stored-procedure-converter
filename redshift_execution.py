import logging
import json
import os, sys
import re
import boto3
import time
import numpy as np
import pandas as pd
import io

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

class RedshiftServerlessQueryExecute:
    def __init__(self, redshift_database_name, redshift_workgroup_name):
        self.redshift_database_name = redshift_database_name
        self.redshift_workgroup_name = redshift_workgroup_name
        self.client = boto3.client("redshift-data")

    def execute_query(self, query_text):
        response = self.client.execute_statement(
            Database="dev",
            WorkgroupName=self.redshift_workgroup_name, 
            Sql=query_text
        )
        query_id = response["Id"]
        return query_id

    def syntax_checker(self, query_id):
        done = False
        while not done:
            time.sleep(1)
            status_description = self.client.describe_statement(Id=query_id)
            status = status_description['Status']
            if status == "FAILED":
                #raise Exception('SQL query failed:' + query_id + ": " + status_description["Error"])
                return "FAILED", status_description["Error"]
            elif status == "FINISHED":
                if status_description['ResultRows']>0:
                    results = self.client.get_statement_result(Id=query_id)
                    column_labels = []
                    for i in range(len(results["ColumnMetadata"])): column_labels.append(results["ColumnMetadata"][i]['label'])
                    records = []
                    for record in results.get('Records'):
                        records.append([list(rec.values())[0] for rec in record])
                    df = pd.DataFrame(np.array(records), columns=column_labels)
                    return "FINISHED", df
                else:
                    return "FINISHED", query_id