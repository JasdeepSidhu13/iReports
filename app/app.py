
# Run this app with `python app.py` and
# visit http://ec2-13-58-63-79.us-east-2.compute.amazonaws.com  in your web browser.

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import pandas as pd



# import the sql and connect libraries for psycopg2
from psycopg2 import sql, connect

# create a global string for the PostgreSQL db name
db_name = "my_db"

try:
    # declare a new PostgreSQL connection object
    conn = connect(
        dbname = db_name,
        user = "your_user_name",
        host = "your_ip",
        password = "your_pwd",
        port = "your_port"
    )

    # print the connection if successful
    print ("psycopg2 connection:", conn)

except Exception as err:
    print ("psycopg2 connect() ERROR:", err)
    conn = None
cur=conn.cursor()

sql_statement = """SELECT "Service_type", "Latitude", "Longitude" \
   FROM requests WHERE "City" = 'SanFrancisco' \
   LIMIT 1000""" 
cur.execute(sql_statement)
cols = cur.fetchall()
#print(cols)
df=pd.DataFrame(cols)
df.columns=["Service_type", "Latitude", "Longitude"]


fig = px.scatter_mapbox(df, lat="Latitude", lon="Longitude", hover_name="Service_type",
hover_data=[ "Service_type"],color_discrete_sequence=["blue"], zoom=5, height=600)
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})


fig.show()


app.layout = html.Div(children=[
    html.H1(children='Hello World'),

    html.Div(children='''
         A visualization of 311 serice request calls in SF.
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])

# Run app
if __name__ == '__main__':
    app.run_server(debug=True, port=8050, host="ec2-13-58-63-79.us-east-2.compute.amazonaws.com")


