import requests
import pandas as pd
import json
from sqlalchemy import create_engine
import os

#script to automate pull of zillow housing data from rapidapi into postgres. Automated daily with cron job

def my_zillow_requester(request):
    # Read environment variables 
    db_user = os.environ.get('USER')
    db_password = os.environ.get('PASSWORD')
    db_host = os.environ.get('IP')
    db_port = os.environ.get('PORT')
    db_name = os.environ.get('DB')

    url = "https://zillow-com1.p.rapidapi.com/propertyExtendedSearch"
    querystring = {"location": "co"}
    headers = {
        "content-type": "application/octet-stream",
        "X-RapidAPI-Key": os.environ.get('API_KEY'),
        "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)
    response_data = json.loads(response.text)
    df = pd.DataFrame(response_data['props'], columns=['propertyType', 'lotAreaValue', 'address', 'priceChange', 'zestimate', 'imgSrc', 'price', 'bedrooms', 'contingentListingType', 'longitude', 'latitude', 'listingStatus', 'zpid', 'rentZestimate', 'daysOnZillow', 'bathrooms', 'livingArea'])
    df = df.fillna(value=0)

    # Establish db connection
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)

    try:
        # Insert the DataFrame into the database
        df.to_sql(name='zillow_data', con=engine, schema='zillow_housing_data', if_exists='replace', index=False)
        return "Data inserted successfully"
    except Exception as e:
        # Handle exceptions and log errors
        return f"Error occurred: {str(e)}"
    finally:
        # Close connection
        engine.dispose()