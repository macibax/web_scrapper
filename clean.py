
from wsgiref.validate import validator
import pandas as pd
import argparse
from urllib.parse import urlparse
import hashlib # para generar hashes (uids)

# configuracin de logger
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(ds_file):
    logger.info("main...")

    # leer dataset con funcion read_csv de pandas
    ds = _read_data(ds_file)

    # obtener host del nombre del dataset
    name = _get_name(ds_file)

    # agregar columna nombre al dataset
    ds = _add_name_column(ds, name)

    # agregar columna host, la cual se obtendra del url de la fila del dataset
    ds = _add_host_column(ds)

    # obtener titulos que no se encontraron (eluniversal)
    ds = _fill_missing_titles(ds)

    # generar uids para las rows
    ds = _generate_uids_for_rows(ds)

    ds = _remove_new_lines_from_body(ds)
    ds = _remove_returns_from_body(ds)

    return ds

def _remove_new_lines_from_body(ds):
    logger.info("removing \n from body")
    cleaned_body = (
        ds.apply(lambda row: list(row['body']), axis=1)
        .apply(lambda letters: "".join(list(map(lambda letter: letter.replace('\n',''), letters))).strip())
    )
    ds['body'] = cleaned_body
    return ds

def _remove_returns_from_body(ds):
    logger.info("removing \r from body")
    cleaned_body = (
        ds.apply(lambda row: list(row['body']), axis=1)
        .apply(lambda letters: "".join(list(map(lambda letter: letter.replace('\r',''), letters))).strip())
    )
    ds['body'] = cleaned_body
    return ds

def _read_data(ds_file):
    logger.info("Reading file....")
    return pd.read_csv(ds_file)

def _get_name(ds_file_name):
    logger.info("Getting name....")
    return ds_file_name.split('_')[0]

def _add_name_column(ds, name):
    logger.info("Adding name column to dataset...")
    ds['name'] = name
    return ds

def _add_host_column(ds):
    logger.info("Adding host column to dataset....")
    # extrear host de la url
    ds['host'] = ds['url'].apply( lambda url: urlparse(url).netloc)
    return ds

def _fill_missing_titles(ds):
    # para el caso de eluniversal
    # se puede extraer el titulo de la ultima seccion
    # del url
    logger.info('Filling missing titles')
    # funcion de pandas que devuelve serie indicando indice y un valor
    # bool true si esta vacio o no
    missing_titles_mask = ds['title'].isna()
    missing_titles = (
        ds[missing_titles_mask]['url']
        .str.extract(r'(?P<missing_titles>[^/]+)$')
        .applymap(lambda title: title.replace('-',' '))
    )

    # asignar todos los totulos del dataset missing_titles a ds
    ds.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']

    return ds

def _generate_uids_for_rows(ds):
    logger.info("Generating uids...")
    # axis = 1 rows
    # axis = 0 columns
    uids = (
        ds.apply(lambda row: hashlib.md5(row['url'].encode()), axis=1)
        .apply(lambda hash_object: hash_object.hexdigest())
    )
    ds['uid'] = uids
    # set_index remplaza los indices con los de la columna indicada
    return ds.set_index('uid')

if __name__ == "__main__":
    logger.info("starting process...")
    parser = argparse.ArgumentParser()

    # agrega el argumento
    parser.add_argument(
        'ds_file',
        help='Path to dirty dataset file',
        type=str,
    )
    args = parser.parse_args()
    ds = main(args.ds_file)
    print(ds)
    # print(ds.isna())