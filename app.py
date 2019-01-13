import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

from flask import Flask, jsonify
from datetime import datetime, date

#################################################
# Database Setup
#################################################
# engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# # reflect an existing database into a new model
# Base = automap_base()
# # reflect the tables
# Base.prepare(engine, reflect=True)

# # Save references to each table
# Measurement = Base.classes.measurement
# Station = Base.classes.station

# # Create our session (link) from Python to the DB
# session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"Available Routes:<br/>"
        f"<a href = 'http://127.0.0.1:5000/api/v1.0/stations'> /api/v1.0/stations</a> Return a list of all stations<br/>"
        f"<a href = 'http://127.0.0.1:5000/api/v1.0/precipitation'> /api/v1.0/precipitation</a> Return a list of the last 12 months of precipitation data of the most active station including the date and prcp<br/>"
        #f"/api/v1.0/precipitation/station Return a list of the last 12 months of precipitation data of the requested station including the date and prcp<br/>"        
        f"<a href = 'http://127.0.0.1:5000/api/v1.0/tobs'> /api/v1.0/tobs</a> Return a list of the dates and temperature observations from a year from the latest data point of the most active station<br/>"
        #f"/api/v1.0/start<br/>"
        #f"/api/v1.0/start/end<br/>"
    )


@app.route("/api/v1.0/stations")
def stations():
    engine = create_engine("sqlite:///Resources/hawaii.sqlite")

    # reflect an existing database into a new model
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)

    # Save references to each table
    Station = Base.classes.station

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of all stations"""
    # Query all stations
    results = session.query(Station.station).all()

    # Convert list of tuples into normal list
    all_stations = list(np.ravel(results))

    return jsonify(all_stations)


@app.route("/api/v1.0/precipitation")
def precipitation():

    engine = create_engine("sqlite:///Resources/hawaii.sqlite")

    # reflect an existing database into a new model
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)

    # Save references to each table
    Measurement = Base.classes.measurement
    Station = Base.classes.station

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of the last 12 months of precipitation data of the most active station including the date and prcp"""
    # Design a query to retrieve the last 12 months of precipitation data of the most active station
    most_active_station = session.query(Measurement.station).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).\
        limit(1).scalar()

    floor_date = datetime.strptime(session.query(Measurement.date).\
        order_by(Measurement.date.desc()).limit(1).scalar()
        ,'%Y-%m-%d')
    ceiling_date = date(floor_date.year -1, floor_date.month, floor_date.day).strftime('%Y-%m-%d')


    # Query precipitation data
    sel = [Measurement.station, Station.name, Measurement.date, Measurement.prcp]
    results = session.query(*sel).\
        filter(Measurement.station == Station.station).\
        filter(Measurement.station == most_active_station).\
        filter(Measurement.date >= ceiling_date).\
        all()

    # Create a dictionary from the row data and append to a list of all_passengers
    all_precipitations = []
    for precipitation in results:
        precipitation_dict = {}
        #precipitation_dict["station"] = precipitation.station
        #precipitation_dict["station_name"] = precipitation.name
        precipitation_dict["date"] = precipitation.date
        precipitation_dict["prcp"] = precipitation.prcp
        all_precipitations.append(precipitation_dict)

    return jsonify(all_precipitations)


@app.route("/api/v1.0/precipitation/<station>")
def precipitation_for_station(station):
    engine = create_engine("sqlite:///Resources/hawaii.sqlite")

    # reflect an existing database into a new model
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)

    # Save references to each table
    Measurement = Base.classes.measurement
    Station = Base.classes.station

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of the last 12 months of precipitation data of the station requested including the date and prcp"""
    # Design a query to retrieve the last 12 months of precipitation data of the requested station

    floor_date = datetime.strptime(session.query(Measurement.date).\
        order_by(Measurement.date.desc()).limit(1).scalar()
        ,'%Y-%m-%d')
    ceiling_date = date(floor_date.year -1, floor_date.month, floor_date.day).strftime('%Y-%m-%d')


    # Query precipitation data
    sel = [Measurement.station, Station.name, Measurement.date, Measurement.prcp]
    results = session.query(*sel).\
        filter(Measurement.station == Station.station).\
        filter(Measurement.station == station).\
        filter(Measurement.date >= ceiling_date).\
        all()

    # Create a dictionary from the row data and append to a list of all_passengers
    all_precipitations = []
    for precipitation in results:
        precipitation_dict = {}
        precipitation_dict["station"] = precipitation.station
        precipitation_dict["station_name"] = precipitation.name
        precipitation_dict["date"] = precipitation.date
        precipitation_dict["prcp"] = precipitation.prcp
        all_precipitations.append(precipitation_dict)

    return jsonify(all_precipitations)



@app.route("/api/v1.0/tobs")
def tobs():

    engine = create_engine("sqlite:///Resources/hawaii.sqlite")

    # reflect an existing database into a new model
    Base = automap_base()
    # reflect the tables
    Base.prepare(engine, reflect=True)

    # Save references to each table
    Measurement = Base.classes.measurement
    Station = Base.classes.station

    # Create our session (link) from Python to the DB
    session = Session(engine)

    """Return a list of the last 12 months of tobs data of the most active station including the date and tobs"""
    # Design a query to retrieve the last 12 months of tobs data of the most active station
    most_active_station = session.query(Measurement.station).\
        group_by(Measurement.station).\
        order_by(func.count(Measurement.station).desc()).\
        limit(1).scalar()

    floor_date_for_most_active_station = datetime.strptime(session.query(func.max(Measurement.date)).\
        filter(Measurement.station == most_active_station).scalar()
        ,'%Y-%m-%d')

    ceiling_date_for_most_active_station = date(floor_date_for_most_active_station.year -1
                                                , floor_date_for_most_active_station.month
                                                , floor_date_for_most_active_station.day).strftime('%Y-%m-%d')
    ceiling_date_for_most_active_station


    # Query tobs data
    sel = [Measurement.station, Station.name, Measurement.date, Measurement.tobs]
    results = session.query(*sel).\
        filter(Measurement.station == Station.station).\
        filter(Measurement.station == most_active_station).\
        filter(Measurement.date >= ceiling_date_for_most_active_station).\
        all()

    # Create a dictionary from the row data and append to a list of all_passengers
    all_rows = []
    for row in results:
        tobs_dict = {}
        #tobs_dict["station"] = row.station
        #tobs_dict["station_name"] = row.name
        tobs_dict["date"] = row.date
        tobs_dict["tobs"] = row.tobs
        all_rows.append(tobs_dict)

    return jsonify(all_rows)

if __name__ == '__main__':
    app.run(debug=True)
