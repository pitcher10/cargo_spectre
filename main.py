import requests
import json
from sqlalchemy import create_engine, Column, Integer, String, DateTime, func
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime 
from sqlalchemy.exc import IntegrityError
from datetime import timedelta

continue_search = True 
#On first run set this to desired date time
first_run_date = "2023-10-10 16:06:22"
# Run the loop while results exist. Spectre API only allows 60 records at a time
while continue_search == True:
    # Define your database path
    database_path = 'sqlite:///cargodata.db'
    engine = create_engine(database_path)

    Base = declarative_base()

    class SearchResults(Base):
        __tablename__ = 'SearchResults'

        cargoID = Column(Integer, primary_key=True)
        barcode = Column(String)
        scanDate = Column(DateTime, nullable=False)
        updated = Column(String)

    # Create the table in the database if it doesn't exist
    Base.metadata.create_all(engine)

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    max_datetime = session.query(func.max(SearchResults.scanDate)).scalar()
    if not max_datetime:
        max_datetime = first_run_date
    else:
        max_datetime = max_datetime + timedelta(seconds=1)

    original_datetime_str = str(max_datetime)
    parsed_datetime = datetime.strptime(original_datetime_str, "%Y-%m-%d %H:%M:%S")
    desired_format_str = parsed_datetime.strftime("%Y-%m-%dT%H:%M:%S")
    print(desired_format_str)
    # API request to get data
    url = "https://spectre-licensing.com/api/cargo/search"
    payload = json.dumps({
        "email": "dock@willystrucking.com",
        "password": "D0ckW1lly5",
        "dateFrom": desired_format_str
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.post(url, headers=headers, data=payload)
    json_response = json.loads(response.text)
    search_results = json_response['searchResults']
    if search_results == None:
        continue_search = False
        print("stopping")
        break

    # Iterate through the search results and add them to the database
    for i in search_results:
        barcode = i['barcode']
        cargoID = i['cargoID']
        scanDate = datetime.fromisoformat(i['scanDate'])

        cargo_data = SearchResults(cargoID=cargoID, barcode=barcode, scanDate=scanDate)

        try:
            session.add(cargo_data)
            session.commit()
        except IntegrityError:
            # Skip the iteration if cargoID is not unique
            session.rollback()
            continue

    # Commit the changes and close the session
    session.commit()
    session.close()