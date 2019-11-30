import argparse
import logging
from urllib.parse import urlparse
import pandas as pd
import hashlib
import datetime
import nltk
from nltk.corpus import stopwords

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(filename) -> pd.DataFrame:
    df = None
    try:
        logger.info('Starting cleaning process')
        df = _read_data(filename)
        df = _add_newspaper_uid_column(df, _extract_newspaper_uid(filename))
        df = _extract_host(df)
        df = _fill_missing_titles(df)
        df = _generate_uids_for_rows(df)
        df = _remove_new_lines_from_body(df)
        df = _tokenize_column(df, 'title')
        df = _tokenize_column(df, 'body')
        df = _remove_duplicate_entries(df, 'title')
        df = _drop_rows_with_missing_values(df)
        _save_data(df, filename)
    except FileNotFoundError as e:
        logger.info('Unable to generate the DataFrame')
    finally:
        return df


def _drop_rows_with_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    logger.info('Dropping rows with missing values')
    return df.dropna()


def _save_data(df: pd.DataFrame, filename: str):
    clean_filename = f'clean_{filename}'
    logger.info(f'Saving data at location: {clean_filename}')
    df.to_csv(clean_filename)


def _remove_duplicate_entries(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    logger.info('Removing duplicate entries')
    df.drop_duplicates(subset=[column_name], keep='first', inplace=True)
    return df


def _tokenize_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    logger.info(f'Calculating the number of unique tokens in {column_name}')
    stop_words = set(stopwords.words('spanish'))
    df['n_tokens_' + column_name] = (
        df.dropna()  # Eliminar NaN y espacios vacios
        .apply(lambda row: nltk.word_tokenize(row[column_name]), axis=1)
        .apply(lambda tokens: list(filter(lambda token: token.isalpha(), tokens)))
        .apply(lambda tokens: list(map(lambda token: token.lower(), tokens)))
        .apply(lambda word_list: list(filter(lambda word: word not in stop_words, word_list)))
        .apply(lambda valid_word_list: len(valid_word_list))
    )
    return df


def _remove_new_lines_from_body(df: pd.DataFrame) -> pd.DataFrame:
    logger.info('Removing new lines from body')
    df['body'] = (
        df.apply(lambda row: row['body'], axis=1)
        .apply(lambda body: str(body).replace('\n', ''))
        .apply(lambda body: str(body).replace('\r', ''))
    )
    return df


def _generate_uids_for_rows(df: pd.DataFrame) -> pd.DataFrame:
    logger.info('Generating uids for each row')
    df['uid'] = (
        df.apply(lambda row: hashlib.md5(bytes(row['url'].encode())), axis=1)
        .apply(lambda hash_object: hash_object.hexdigest())
    )
    return df.set_index('uid')


def _fill_missing_titles(df: pd.DataFrame) -> pd.DataFrame:
    logger.info('Filling missing titles')
    missing_titles_mask = df['title'].isna()
    missing_titles = (
        df[missing_titles_mask]['url']
        .str.extract(r'(?P<missing_titles>[^/]+)$')
        .applymap(lambda title: title.replace('-', ' '))
    )
    df.loc[missing_titles_mask, 'title'] = missing_titles.loc[:, 'missing_titles']
    return df


def _extract_host(df: pd.DataFrame) -> pd.DataFrame:
    logger.info('Extracting host from urls')
    df['host'] = df['url'].apply(lambda url: urlparse(url).netloc)
    return df


def _add_newspaper_uid_column(df: pd.DataFrame, newspaper_uid: str) -> pd.DataFrame:
    logger.info(f'Filling newspaper_uid column with {newspaper_uid}')
    df['newspaper_uid'] = newspaper_uid
    return df


def _extract_newspaper_uid(filename: str) -> str:
    logger.info('Extracting newspaper uid')
    newspaper_uid = filename.split('_')[0]
    logger.info(f'Newspaper uid detected: {newspaper_uid}')
    return newspaper_uid


def _read_data(filename) -> pd.DataFrame:
    logger.info(f'Reading file {filename}')
    return pd.read_csv(filename)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The path to dirty data',
                        type=str)
    args = parser.parse_args()
    # with pd.option_context('display.max_columns', 5):
    #     with pd.option_context('display.max_rows', 157):
    now = datetime.datetime.now()
    print(main(args.filename))
    print(datetime.datetime.now() - now)
