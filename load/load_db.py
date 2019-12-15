import argparse
import logging
import pandas as pd
from load.article import Article
from load.base import Base, engine, Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(filename):
    try:
        Base.metadata.create_all(engine)
        session = Session()
        articles = pd.read_csv(filename)
        for index, row in articles.iterrows():
            logger.info(f'Loading article uid {row["uid"]} into DB')
            article = Article(row['uid'], row['body'], row['host'], row['newspaper_uid'],
                              row['n_tokens_body'], row['n_tokens_title'], row['title'], row['url'])
            session.add(article)
        session.commit()
        session.close()
    except FileNotFoundError:
        logger.info('No data found')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename',
                        help='The file you want to load into the db',
                        type=str)
    args = parser.parse_args()
    main(args.filename)
