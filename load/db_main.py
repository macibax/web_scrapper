# argparse es un modulo que permite
# utilizar argumentos desde la liena de comandos
import argparse
from ast import arg
from importlib.metadata import metadata 

import pandas as pd

from article import Article
from base import Base, engine, Session

# configuracin de logger
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(filename):
    # generamos el schema en la bd
    Base.metadata.create_all(engine)

    # inicializar session
    session = Session()

    # leer articulos con pandas
    articles = pd.read_csv(filename)

    # iterrows permite generar un loop dentro de cada fila del dataframe
    for index, row in articles.iterrows():
        logger.info(f"Loading article uid {row['uid']} into DB")

        # creamos el articulo, el cual se insertara en la bd
        article = Article(
            row['uid'],
            row['body'],
            row['host'],
            row['name'],
            row['token_body'],
            row['token_title'],
            row['title'],
            row['url']
        )

        # se inserta articulo en la bd
        session.add(article)
    
    # se da commit a la session
    session.commit()

    # finalmente se cierra la session
    session.close()

    # NOTA::: SE PUEDE PROBAR LA BASE EN LINEA EN: https://sqliteonline.com/


if __name__ == "__main__":

    # se preparan argumentos
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'filename',
        help='filename to save into sqlite',
        type=str,
    )
    args = parser.parse_args()
    main(args.filename)