from sqlalchemy import Column, Integer, String, Float, Boolean, Date
from database import Base


class TrainTrip(Base):
    __tablename__ = "train_trips"

    id = Column(Integer, primary_key=True, index=True)
    trip_id = Column(String, nullable=True)
    mode = Column(String, nullable=True)
    source = Column(String, nullable=True)
    departure_city = Column(String, nullable=True)
    departure_country = Column(String, nullable=True)
    departure_station = Column(String, nullable=True)
    departure_time = Column(String, nullable=True)
    arrival_city = Column(String, nullable=True)
    arrival_country = Column(String, nullable=True)
    arrival_station = Column(String, nullable=True)
    arrival_time = Column(String, nullable=True)
    agency_name = Column(String, nullable=True)
    is_night_train = Column(Boolean, nullable=True)
    distance_km = Column(Float, nullable=True)
    distance_m = Column(Float, nullable=True)
    emissions_co2 = Column(Float, nullable=True)
    co2_per_pkm = Column(Float, nullable=True)
    days_of_week = Column(String, nullable=True)
    service_start_date = Column(Date, nullable=True)
    service_end_date = Column(Date, nullable=True)