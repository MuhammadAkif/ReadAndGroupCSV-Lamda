# ReadAndGroupCSV(server less)

# Description

NodeJS script that takes as an input a path to a CSV file containing an arbitrary set of data, and outputs the same data in multiple CSV files, where values got grouped by the value of the column A. Each file should be named after the value of the column A. Output CSV files should not contain the value of column A inside.

An example of input.csv:

Column A,Column B,Column C

1,aaa,qqq

1,bbb,www

2,ccc,eee

2,ddd,rrr

3,eee,ttt

3,fff,yyy

Script should output 3 files:

1.csv:

Column B,Column C

aaa,qqq

bbb,www

2.csv:

Column B,Column C

ccc,eee

ddd,rrr

3.csv:

Column B,Column C

eee,ttt

After that it will upload files 1.csv, 2.csv, 3.csv to your s3 bucket.

# Please follow below steps to run csv converter

To run lamda function you need to upload it to aws, but to test it locally and make development fast we are using a plugin serverless-offline. 
 

- run `serverless offline` 

By default it will run on 3000 port to change it use can use
`serverless offline --port 3000`

You can then hit the server on localhost:<your-port>/csv

It is a post request and below is the body you can send.

`{
    "fileLink": "link-to-your-csv"
}`

# Note
Please add a .env file with below three variables for aws-sdk

SECRET_ACCESS_KEY:
ACCESS_KEY_ID:
REGION:

