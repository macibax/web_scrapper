
# permite usar comandos de terminal desde codigo
import subprocess

# logs
import logging
logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

news_sites_uids = ['elimparcial',]

# en windows no funcina
# este archivo fue pensado en sistemas unix
python_exec = 'python'
# virtual_env_dir = "scraper_env"
# python_exec = f"{virtual_env_dir}/Scripts/python.exe"

def main():
    _extract()
    _transform()
    _load()

def _extract():
    logger.info('Startin extract process...')
    for news_site_uid in news_sites_uids:
        # esta linea permite emular la ejecucion de un
        # comando desde terminal
        subprocess.run(
            [python_exec, 'main.py', news_site_uid],
            cwd='./extract'
        )

        # este comando encuentra el archiuvo que cumpla el patron
        # y lo mueve de carpeta a transform
        subprocess.run(
            [
                'find', '.', '-name', f"{news_site_uid}*",
                '-exec', 'mv', '{}', f"../transform/{news_site_uid}_.csv", ';',
            ],
            cwd='./extract'
        )



def _transform():
    logger.info('Starting transform process...')
    for news_site_uid in news_sites_uids:
        dirty_data_filename = f"{news_site_uid}_.csv"
        clean_data_filename = f"clean_{dirty_data_filename}"

        # ejecuta el archivo de limpiado
        subprocess.run(
            [python_exec, 'clean.py', dirty_data_filename],
            cwd='./transform'
        )

        # eliminar archivo "sucio"
        subprocess.run(
            ['rm', dirty_data_filename],
            cwd='./transform'
        )

        # mueve archivo
        subprocess.run(
            ['mv', clean_data_filename, f"../load/{news_site_uid}.csv"],
            cwd='./transform'
        )

def _load():
    logger.info("Starting load process...")
    for news_site_uid in news_sites_uids:
        clean_data_filename = f"{news_site_uid}.csv"

        # ejecutar archivo para cargar a base de datos
        subprocess.run(
            [python_exec, 'db_main.py', clean_data_filename],
            cwd='./load'
        )

        # eliminar archivo csv
        # subprocess.run(
        #     ['rm', clean_data_filename],
        #     cwd='./load'
        # )

if __name__ == "__main__":
    main()