from sqlalchemy import create_engine

# permite usar el orm de sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# se crea motor de db sqlite
engine = create_engine('sqlite:///newspaper.db')

# se crea objeto sesison con engine sqlite
Session = sessionmaker(bind=engine)

# de aqui extenderan todfos los modelos de sql
Base = declarative_base()