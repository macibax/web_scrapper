
from wsgiref.validate import validator
import pandas as pd
import argparse
from urllib.parse import urlparse
import hashlib # para generar hashes (uids)

# tokenizar
import nltk
from nltk.corpus import stopwords

# nltk.download('punkt') # solo ejecutar 1 vez, despues comentar
# nltk.download('stopwords') # solo ejecutar 1 vez, despues comentar

# seleccionar el idioma para el analisis
stop_words = set(stopwords.words('spanish'))

# configuracin de logger
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# para guardar en csv
# from common import save_csv_dataset


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

    # eliminar \r y \n de titulo y body
    ds = _remove_new_lines_from_column(ds, 'title')
    ds = _remove_returns_from_column(ds, 'title')
    ds = _remove_new_lines_from_column(ds, 'body')
    ds = _remove_returns_from_column(ds, 'body')

    # eliminar estacios en blanco al inicio y final
    ds = _strip_column(ds, 'title')
    ds = _strip_column(ds, 'body')

    # ENRIQUEZER DATOS
    # agregar columnas que muestran la cantidad de palabras
    # relevantes para title y body
    ds = _tokenize_column(ds, 'title')
    ds = _tokenize_column(ds, 'body')

    # remover duplicados
    ds = _remove_duplicate_entries(ds, 'title')

    # eliminar rows cond atos vacios
    ds = _drop_rows_with_missing_values(ds)

    return ds

def _drop_rows_with_missing_values(ds):
    logger.info("removing null values")
    return ds.dropna()

def _remove_duplicate_entries(ds, column):
    logger.info("removing dupplicate entries")
    return ds.drop_duplicates(subset=column, keep='first')

def _tokenize_column(ds, column):
    logger.info(f"adding tokenized column {column}")
    tokenized = (
        # nltk.word_tokenize(row[column])
        # funcione que tokeniza el valir de row[column]
        # https://www.nltk.org/api/nltk.tokenize.html
        # Tokenizers divide strings into lists of substrings. For example, tokenizers can be used to find the words and punctuation in a string
        ds.apply(lambda row: nltk.word_tokenize(row[column]), axis=1)
        # obtiene lista de tokens alfanumericos
        .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
        # convertir todo a minusculas para que se pueda comparar con stopwords
        .apply(lambda tokens: map(lambda token: token.lower(), tokens))
        # eliminar las stopwords de la lista
        .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
        # obtener cantidad de palabras validas
        .apply(lambda valid_word_list: len(valid_word_list))
    )

    # agregar columna
    ds[f"token_{column}"] = tokenized
    return ds

def _strip_column(ds, column):
    logger.info("removing white spaces at the beginning and end from column")
    # eliminar espacios en blanco al inicio y final
    stripped = (
        ds.apply(lambda row: row[column].strip(), axis=1)
    )
    ds[column] = stripped
    return ds


def _remove_new_lines_from_column(ds, column):
    logger.info("removing \n from column")
    cleaned_body = (
        ds.apply(lambda row: list(row[column]), axis=1)
        .apply(lambda letters: "".join(list(map(lambda letter: letter.replace('\n',''), letters))).strip())
    )
    ds[column] = cleaned_body
    return ds

def _remove_returns_from_column(ds, column):
    logger.info("removing \r from column")
    cleaned_body = (
        ds.apply(lambda row: list(row[column]), axis=1)
        .apply(lambda letters: "".join(list(map(lambda letter: letter.replace('\r',''), letters))).strip())
    )
    ds[column] = cleaned_body
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

def save_csv(name, ds):
    logger.info("Saving csv...")
    ds.to_csv(f"clean_{name}", encoding = 'utf-8-sig')

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

    # guardar en csv
    # save_csv_dataset(f"clean_{args.ds_file}", ds)
    save_csv(args.ds_file, ds)
    # print(ds.isna())