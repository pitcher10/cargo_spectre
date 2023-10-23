from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import and_, func, not_ 
import requests 
import os

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
results = session.query(SearchResults.report_url, SearchResults.detail_line_id, SearchResults.cargoID).filter(not_(SearchResults.updated.like('%S%'))).all()

for report_url, detail_line_id, cargoID in results:
    report_url = report_url.replace("\\", "/")
    download_url = f"https://cargospectre.blob.core.windows.net/scans/{report_url}"

    # Get the PDF content
    response = requests.get(download_url)
    if response.status_code == 200:
        # Define the target directory where you want to save the PDF files
        target_directory = 'downloaded_pdfs'
        os.makedirs(target_directory, exist_ok=True)

        # Determine the target file name based on detail_line_id
        target_file_name = f"{detail_line_id}.pdf"
        counter = 1

        # Check if the file with the same name already exists and add a counter if necessary
        while os.path.exists(os.path.join(target_directory, target_file_name)):
            target_file_name = f"{detail_line_id}_{counter:02d}.pdf"
            counter += 1

        # Save the PDF file with the determined name
        with open(os.path.join(target_directory, target_file_name), 'wb') as pdf_file:
            pdf_file.write(response.content)

        print(f"Downloaded and saved: {target_file_name}")
        record = session.query(SearchResults).filter(SearchResults.cargoID == cargoID).first()
    
        if record:
            # Update the 'updated' column by appending 'S'
            record.updated = record.updated + 'S'
            
            # Commit the changes to the database
            session.commit()
    else:
        print(f"Failed to download {download_url}")