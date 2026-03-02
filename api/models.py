from sqlalchemy import Column, String, Integer, Float, Boolean, Date
from database import Base


class TrainTrip(Base):
    __tablename__ = "train_trips"

    # Clé primaire composite simulée via un id auto
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)

    # Identifiants source
    source = Column(String, nullable=True)
    trip_id = Column(String, index=True, nullable=True)
    destination = Column(String, nullable=True)
    trip_short_name = Column(String, nullable=True)

    # Agence
    agency_name = Column(String, nullable=True)
    agency_timezone = Column(String, nullable=True)

    # Service / Route
    service_id = Column(String, nullable=True)
    route_id = Column(String, nullable=True)
    route_type = Column(Integer, nullable=True)
    route_short_name = Column(String, nullable=True)
    route_long_name = Column(String, nullable=True)

    # Départ
    departure_station = Column(String, nullable=True)
    departure_city = Column(String, index=True, nullable=True)
    departure_country = Column(String, nullable=True)
    departure_time = Column(String, nullable=True)
    departure_parent_station = Column(String, nullable=True)

    # Arrivée
    arrival_station = Column(String, nullable=True)
    arrival_city = Column(String, index=True, nullable=True)
    arrival_country = Column(String, nullable=True)
    arrival_time = Column(String, nullable=True)
    arrival_parent_station = Column(String, nullable=True)

    # Calendrier
    service_start_date = Column(Date, nullable=True)
    service_end_date = Column(Date, nullable=True)
    days_of_week = Column(String, nullable=True)
    is_night_train = Column(Boolean, nullable=True)

    # Métriques CO2
    distance_m = Column(Float, nullable=True)
    co2_per_pkm = Column(Float, nullable=True)
    emissions_co2 = Column(Float, nullable=True)