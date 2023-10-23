from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import and_, func
import requests 
import json 
from tm_functions import get_dlid, update_dims, delete_previous_dims, get_sequence, update_tlorder, check_to_be_deleted, delete_old_dtls, check_webservice, update_weight, insert_admin_status, update_admin_status
from datetime import datetime

now = datetime.now()

formatted_timestamp = now.strftime("%Y-%m-%d-%H.%M.%S")

# Define your database path
database_path = 'sqlite:///cargodata.db'
engine = create_engine(database_path)

Base = declarative_base()

class SearchResults(Base):
    __tablename__ = 'SearchResults'

    cargoID = Column(Integer, primary_key=True)
    barcode = Column(String)
    scanDate = Column(DateTime, nullable=False)
    length = Column(Float)
    width = Column(Float)
    height = Column(Float)
    weight = Column(Float)
    updated = Column(String)
    report_url = Column(String)
    detail_line_id = Column(Integer)

# Create the table in the database if it doesn't exist
Base.metadata.create_all(engine)

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

# Query for cargoID where 'updated' is null
result = session.query(SearchResults.cargoID).filter(SearchResults.updated == None).all()

# Get dimesions from spectre cloud
def api_call(cargoId):
    url = "https://spectre-licensing.com/api/cargo/export"
    payload = json.dumps({
        "email": "dock@willystrucking.com",
        "password": "D0ckW1lly5",
        "cargoId": cargoId
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.post(url, headers=headers, data=payload)
    json_response = json.loads(response.text)
    search_results = json_response['responses']
    return search_results

# Update truckmate tldtl_dimensions

for row in result:
    cargoID = row[0]
    api_info = api_call(cargoId=cargoID)
    dimensions = api_info["dimension"]["info"]["dimensions"]
    barcode = api_info["dimension"]["info"]["barcode"]
    barcode = barcode.split("-")[0]
    if barcode:
        dlid = get_dlid(barcode=barcode)
        if dlid:

            dlid = dlid[0][0]
            sequence = get_sequence(dlid)[0][0]
            length = dimensions['length']
            width = dimensions['width']
            height = dimensions['height']
            weight = dimensions['weight']['net']
            report_url = api_info["snapshot"]["directory"]["misc"]["path"][0]
            if check_webservice(dlid=dlid):
                session.query(SearchResults).filter(SearchResults.cargoID == cargoID).update({"updated": "X","weight":weight,"length":length,"width":width,
                                                                                          "height":height,"report_url":report_url,"detail_line_id":dlid})
                session.commit()

            else:
                delete_previous_dims(dlid=dlid)
                update_dims(dlid=dlid,length=length,width=width,height=height,sequence=sequence)
                update_tlorder(dlid=dlid)
                # Add status update 
                print(dlid)
                print(dimensions)
                session.query(SearchResults).filter(SearchResults.cargoID == cargoID).update({"updated": "D","weight":weight,"length":length,"width":width,
                                                                                            "height":height,"report_url":report_url,"detail_line_id":dlid})
                session.commit()

#update weight
weight_result = session.query(SearchResults.detail_line_id, func.sum(SearchResults.weight))\
                  .filter(SearchResults.updated == 'D')\
                  .group_by(SearchResults.detail_line_id).all()

for detail_line_id, weight_sum in weight_result:
    print(detail_line_id,weight_sum)
    update_weight(dlid=detail_line_id,weight=weight_sum)
    insert_admin_status(dlid=detail_line_id,current_timestamp=formatted_timestamp)
    update_admin_status(dlid=detail_line_id)
    session.query(SearchResults).filter(SearchResults.detail_line_id == detail_line_id).update({"updated": "DW"})
    session.commit()
# Close the session
session.close()