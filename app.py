#!bin/env python2.7

from celery import Celery
import pandas as pd

app = Celery('jobs', broker='amqp')

@app.task
def summarize_file(file_path):
    pd.read_csv(file_path, header=None).max().to_csv('summary_' + file_path)