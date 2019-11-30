import logging
import subprocess
import os
import shutil

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
news_sites_uids = ['eluniversal', 'elpais']


def _load():
    logger.info('Starting load process')
    for news_site_uid in news_sites_uids:
        clean_data_filename = _search_file('.\\load', news_site_uid)
        subprocess.run(['python', 'load_db.py', clean_data_filename], cwd='./load')
        subprocess.run(['del', clean_data_filename], cwd='./load')  # Windows: del | Unix: rm | Eliminar un archivo


def _extract():
    logger.info('Starting extract process')
    for news_site_uid in news_sites_uids:
        # subprocess.run(['python', 'newspaper_created.py', news_site_uid], cwd='./extract')
        path = '.\\extract'
        file = _search_file(path, news_site_uid)
        _move_file(path + '\\' + file, '.\\transform\\' + file)


def _search_file(path, file_match):
    logger.info('Searching file')
    for rutas in list(os.walk(path))[0]:
        if len(rutas) > 1:
            for file in rutas:
                if file_match in file:
                    return file
    return None


def _move_file(origen, destino):
    logger.info('Moving file')
    shutil.move(origen, destino)


def _transform():
    logger.info('Starting transform process')
    for news_site_uid in news_sites_uids:
        dirty_data_filename = _search_file('.\\transform', news_site_uid)
        clean_data_filename = f'clean_{dirty_data_filename}'
        subprocess.run(['python', 'newspaper_receipe.py', dirty_data_filename], cwd='./transform')
        subprocess.run(['del', dirty_data_filename], cwd='./transform')  # Windows: del | Unix: rm | Eliminar un archivo
        _move_file('.\\transform\\' + clean_data_filename, '.\\load\\' + clean_data_filename)


def main():
    try:
        _extract()
        _transform()
        _load()
    except:
        logger.warning('Error process')


if __name__ == '__main__':
    main()
