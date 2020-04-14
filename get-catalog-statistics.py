import csv
import datetime
import logging
import lxml.etree
import os
import pathlib
import requests
import requests.auth
import sys

log = logging.getLogger(__name__)


class Settings:
    @property
    def basic_auth(self) -> requests.auth.HTTPBasicAuth:
        return requests.auth.HTTPBasicAuth(self.username, self.password)

    @property
    def catalog_hostname(self) -> str:
        return os.getenv('CATALOG_HOST', 'http://example.com:9085')

    @property
    def log_format(self) -> str:
        return os.getenv('LOG_FORMAT', '%(levelname)s [%(name)s] %(message)s')

    @property
    def log_level(self) -> str:
        return os.getenv('LOG_LEVEL', 'INFO')

    @property
    def output_file_prefix(self) -> str:
        return os.getenv('OUTPUT_FILE_PREFIX', '')

    @property
    def output_folder(self) -> pathlib.Path:
        return pathlib.Path(os.getenv('OUTPUT_FOLDER', '/data')).resolve()

    @property
    def password(self) -> str:
        return os.getenv('PASSWORD')

    @property
    def username(self) -> str:
        return os.getenv('USERNAME')

    @property
    def version(self) -> str:
        return os.getenv('VERSION')


def get_raw_data(settings: Settings) -> lxml.etree.Element:
    url = f'{settings.catalog_hostname}/access/1/catalog/eicstats'
    r = requests.get(url, auth=settings.basic_auth)
    return lxml.etree.fromstring(r.text)


def yield_login_stats(xml: lxml.etree.Element):
    for day in xml.find('UsageStats/UserActivity/loginActivity/edcLoginStats'):
        for user_login_timestamp in day.findall('userLoginTimestamps'):
            user_id = user_login_timestamp.find('userId').text
            for timestamp in user_login_timestamp.findall('loginTimestamp'):
                login_timestamp = datetime.datetime.strptime(timestamp.text, '%a %b %d %H:%M:%S %Z %Y')
                yield {'user_id': user_id, 'login_timestamp': login_timestamp}


def main():
    settings = Settings()
    logging.basicConfig(format=settings.log_format, level='DEBUG', stream=sys.stdout)
    log.debug(f'get-catalog-statistics {settings.version}')
    if not settings.log_level == 'DEBUG':
        log.debug(f'Changing log level to {settings.log_level}')
    logging.getLogger().setLevel(settings.log_level)

    data = get_raw_data(settings)
    login_timestamps_file = settings.output_folder / f'{settings.output_file_prefix}catalog-login-timestamps.csv'
    with login_timestamps_file.open('w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['user_id', 'login_timestamp'])
        writer.writeheader()
        for t in yield_login_stats(data):
            writer.writerow(t)


if __name__ == '__main__':
    main()
