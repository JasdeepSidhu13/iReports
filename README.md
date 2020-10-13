# iReports
This Readme is a work in progress.

This is a project I built while working as Insight Data Engineering Fellow. The project creates a multi-city 311 database.


## Motivation

The 311 is a service which allows citizens to inform government of non-emergency problems or service requests facing their neighborhood. The big challenge for the government agencies is how to prioritize efficient allocation of resources.Building a nationwide 311 database can be of great use to address this problem. It will not only allow the government agencies to allocate resources efficiently but also help make them long term data driven decisions and serve people better. 


## Datasets

I have collected data from ten cities - The data is obtained from the official Open data web pages of the cities. The combined data is about 20 GB in size and has over 30 million rows. 


## Architecture 

For this project I used S3 for storing collected data, Apache Spark for processing, Postgres to build the database and Plotly Dash for visualization. All of them are hosted on AWS EC2 instances.  

![alt text](docs/tech_stack.png "hover text")




## Web App Instructions 

Edit the last line in `dash/dash_app.py` to server the app over local or ec2 pg instance and launch the web app with 
```
python app/app.py 
```
and browse to 
```
http://127.0.0.1:8050/
or
http://ec2-13-58-63-79.us-east-2.compute.amazonaws.com:8050/
```
