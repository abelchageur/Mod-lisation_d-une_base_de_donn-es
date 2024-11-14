import os
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Numeric, DateTime, ForeignKey  # Added ForeignKey here
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import insert, select
from sqlalchemy.orm import relationship, sessionmaker, Session
from sqlalchemy.exc import NoResultFound
import pandas as pd  

df = pd.read_csv('briff_3_app.csv')

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:pass@localhost:5533/alfha")
engine = create_engine(DATABASE_URL)
 
Base = declarative_base()

class Ville(Base):
    __tablename__ = 'Ville'
    id = Column(Integer, primary_key=True)
    name_ville = Column(String, unique=True)
    annonces = relationship("Annonce", back_populates="city")

class Equipement(Base):
    __tablename__ = 'Equipement'
    id = Column(Integer, primary_key=True)
    name_equipement = Column(String, unique=True)
    annonces = relationship("AnnonceEquipement", back_populates="equipement")

class Annonce(Base):
    __tablename__ = 'Annonce'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    price = Column(Numeric(10, 2))
    nb_rooms = Column(Integer)
    num_bathrooms = Column(Integer)
    surface_area = Column(Numeric(10, 2))
    link = Column(String)
    city_id = Column(Integer, ForeignKey('Ville.id'))
    city = relationship("Ville", back_populates="annonces")
    equipements = relationship("AnnonceEquipement", back_populates="annonce")

class AnnonceEquipement(Base):
    __tablename__ = 'Annonce_Equipement'
    Annonce_id = Column(Integer, ForeignKey('Annonce.id'), primary_key=True)
    equipement_id = Column(Integer, ForeignKey('Equipement.id'), primary_key=True)
    annonce = relationship("Annonce", back_populates="equipements")
    equipement = relationship("Equipement", back_populates="annonces")


Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)

def get_or_create_city(city_name):
    city = session.query(Ville).filter_by(name_ville=city_name).first()
    if not city:
        city = Ville(name_ville=city_name)
        session.add(city)
        session.commit()  # Save immediately to get the ID
    return city

def get_or_create_equipement(equipment_name):
    equipement = session.query(Equipement).filter_by(name_equipement=equipment_name).first()
    if not equipement:
        equipement = Equipement(name_equipement=equipment_name)
        session.add(equipement)
        session.commit()  # Save immediately to get the ID
    return equipement

for index, row in df.iterrows():
    city = get_or_create_city(row['Localisation_appart'])

    annonce = Annonce(
        title=row['Titre_appart'],
        price=row['Prix_appart'],
        nb_rooms=row['N_chambre'],
        num_bathrooms=row['N_douches'],
        surface_area=row['Surface_habitable'],
        link=row['Link_appart'],
        city=city
    )
    session.add(annonce)
    session.commit()

    equipements = row['équipements'].split(',')
    for equipement_name in equipements:
        equipement_name = equipement_name.strip()
        equipement = get_or_create_equipement(equipement_name)

        annonce_equipement = AnnonceEquipement(annonce=annonce, equipement=equipement)
        session.add(annonce_equipement)

try:
    session.commit()
    print("Data inserted successfully.")
except IntegrityError:
    session.rollback()
    print("An error occurred with data insertion.")
finally:
    session.close()

