import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pyspark import *
from pyspark.sql import Row, DataFrame, SQLContext
from Pycrypto import Spark_Session


def readCsv_writeCsv():
    df = pd.read_csv("/Users/shuvamoymondal/Downloads/train.csv",encoding = 'utf_8_sig')
    print(df.dtypes)
    print(df.get_dtype_counts())
    #print(df)
    plt.plot(df[['ApplicantIncome']],df[['ApplicantIncome']],'r--')
    df[['ApplicantIncome','Loan_ID', 'Gender', 'Married']].to_csv('merged.csv', encoding = 'utf-8')
    print(df.head(2))
#print(df.describe())
#v = sql.read.csv("/Users/shuvamoymondal/Downloads/train.csv")

#df['ApplicantIncome'].hist(bins=50)
#df['Property_Area'].value_counts()
#df.boxplot(column='ApplicantIncome', by = 'Education')

if __name__ == '__main__':
    sprk = Spark_Session()
    conn = sprk.Spark_Context()
    sql_conn=sprk.Spark_Connect()
    sql = SQLContext(conn)
readCsv_writeCsv()
ndf = sql.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("merged.csv")
ndf.show()
