import time
import pandas as pd  
import plotly.express as px 
import plotly.graph_objects as go
import streamlit as st  
from datetime import datetime
import mysql.connector
from sqlalchemy import create_engine,text


st.set_page_config(
  page_title="Sales Dashboard",
  page_icon="✅",
  layout="wide",
)

config = {
        'user': 'root',
        'password': 'debezium',
        'host': '127.0.0.1',
        'port': '3307',
        'database': 'myCompany'
    }


def loadData():
    connection = mysql.connector.connect(**config)
    # Create an SQLAlchemy engine using the connection
    engine = create_engine('mysql+mysqlconnector://root:debezium@127.0.0.1:3307/myCompany')
    tableName = "speedView_" + str(datetime.now().day) + "_" + str(datetime.now().month) + "_" + str(datetime.now().year)
    query = text("SELECT * FROM {} order by product_id".format(tableName))
    # query = "SELECT * FROM speedView_8_11_2023 order by product_id"
    with engine.connect() as conn:
        df = pd.read_sql_query(query, con=conn)
    connection.close()
    return df

df = loadData()
st.sidebar.header("Choose your filter: ")
# Create for Model
model = st.sidebar.multiselect("Pick your Model", df["Model"].unique())
if not model:
    modelDF = df.copy()
else:
    modelDF = df[df["Model"].isin(model)]


# Create for State
category = st.sidebar.multiselect("Pick the Category", modelDF["Category"].unique())
if not category:
    categoryDF = modelDF.copy()
else:
    categoryDF = modelDF[modelDF["Category"].isin(category)]

# creating a single-element container
placeholder = st.empty()

while True:
  with placeholder.container():
    df = loadData()

    if not model and not category:
        filteredDF = df
    elif model and category:
        filteredDF = categoryDF[df["Model"].isin(model) & categoryDF["Category"].isin(category)]
    elif model:
        filteredDF = modelDF
    else:
        filteredDF = df[df["Category"].isin(category)]

    st.markdown("<h1 style='text-align: center; margin: 10px; padding: 0px 0px 2rem'>Real-Time Sales Dashboard</h1>", unsafe_allow_html=True)

    total = filteredDF['Revenue'].sum()
    sold = filteredDF['Sold'].sum()

    kpi1, kpi2 = st.columns(2)

    kpi1.metric(
        label="Total Sale Amount",
        value=f"{total} $"
        )
    kpi2.metric(
        label="Total Car Sold",
        value=f"{sold}"
        )
    st.divider()

    fig_col1, fig_col2 = st.columns(2)

    with fig_col1:
        st.subheader("Sold by Make")
        sumOfMake = filteredDF.groupby('Make', as_index = False)['Sold'].sum().sort_values(by='Sold',ascending = False)
        sumOfMake = sumOfMake.head(10)
        fig = px.bar(sumOfMake, x='Sold', y='Make')
        fig.update_layout(yaxis=dict(autorange="reversed"),margin=dict(t=30))
        st.write(fig)


    with fig_col2:
        st.subheader("Payment methods")
        fig = px.pie(filteredDF, values= filteredDF['payment'].value_counts(), names= filteredDF['payment'].unique())
        fig.update_layout(margin=dict(l=60, r=20, t=30))
        st.write(fig)

    idSoldDF = filteredDF.groupby(['product_id', 'Make', 'Model', 'Category', 'inv_quantity'], as_index = False)['Sold'].sum().sort_values(by='product_id',ascending = True)
    # idSoldDF = idSoldDF.rename(columns={'Sold': 'SoldById'})
    idSoldDF['LeftOver'] = idSoldDF['inv_quantity'] - idSoldDF['Sold']
    idSoldDF = idSoldDF[['product_id', 'Make', 'Model', 'Category', 'Sold', 'LeftOver']]
    idSoldDF = idSoldDF.sort_values(by='LeftOver',ascending = True)
    fig = go.Figure(data=[go.Table(
    columnwidth = [80,200,200,200,80],
            header=dict(values=list(idSoldDF.columns),
                    fill_color='paleturquoise',
                    align='center',
                    font=dict(size=15)),
            cells=dict(values=[idSoldDF.product_id, idSoldDF.Make, idSoldDF.Model, idSoldDF.Category, idSoldDF.Sold, idSoldDF.LeftOver],
                fill_color='lavender',
                align='left',
                font_size=15,
                height=30))
        ])
    fig.update_layout(margin=dict(t=30))
    st.subheader("Inventory quantity")
    st.plotly_chart(fig, use_container_width=True)
    time.sleep(15)
