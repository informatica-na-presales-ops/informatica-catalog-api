import apscheduler.schedulers.blocking
import datetime
import fort
import logging
import lxml.etree
import os
import requests
import requests.auth
import signal
import sys

log = logging.getLogger(__name__)


class Database(fort.PostgresDatabase):
    def __init__(self, settings, dsn):
        super().__init__(dsn)
        self.settings = settings

    def add_user_login_timestamp(self, user_id: str, login_timestamp: datetime.datetime):
        sql = '''
            select event_id from environment_usage_events
            where environment_name = %(environment_name)s
              and event_name = %(event_name)s
              and user_id = %(user_id)s
              and event_time = %(event_time)s
        '''
        params = {
            'environment_name': self.settings.environment_name,
            'event_name': 'login',
            'user_id': user_id,
            'event_time': login_timestamp
        }
        existing = self.q_val(sql, params)
        if existing is None:
            self.log.info(f'Adding event to database: {user_id} at {login_timestamp}')
            sql = '''
                insert into environment_usage_events (
                    environment_name, event_name, user_id, event_time
                ) values (
                    %(environment_name)s, %(event_name)s, %(user_id)s, %(event_time)s
                )
            '''
            self.u(sql, params)
        else:
            self.log.info(f'This event is already in the database: {user_id} at {login_timestamp}')


class Settings:
    _true_values = ('true', '1', 'on', 'yes')

    @property
    def basic_auth(self) -> requests.auth.HTTPBasicAuth:
        return requests.auth.HTTPBasicAuth(self.username, self.password)

    @property
    def catalog_hostname(self) -> str:
        return os.getenv('CATALOG_HOST', 'http://example.com:9085')

    @property
    def db(self) -> Database:
        return Database(self, os.getenv('DB'))

    @property
    def environment_name(self) -> str:
        return os.getenv('ENVIRONMENT_NAME')

    @property
    def log_format(self) -> str:
        return os.getenv('LOG_FORMAT', '%(levelname)s [%(name)s] %(message)s')

    @property
    def log_level(self) -> str:
        return os.getenv('LOG_LEVEL', 'INFO')

    @property
    def password(self) -> str:
        return os.getenv('PASSWORD')

    @property
    def run_and_exit(self) -> bool:
        return os.getenv('RUN_AND_EXIT', 'False').lower() in self._true_values

    @property
    def sync_interval_hours(self) -> int:
        return int(os.getenv('SYNC_INTERVAL_HOURS', '12'))

    @property
    def username(self) -> str:
        return os.getenv('USERNAME')

    @property
    def version(self) -> str:
        return os.getenv('VERSION')


def get_raw_data(settings: Settings) -> lxml.etree.Element:
    url = f'{settings.catalog_hostname}/access/1/catalog/eicstats'
    log.info(f'Requesting {url}')
    r = requests.get(url, auth=settings.basic_auth)
    return lxml.etree.fromstring(r.text)


def yield_login_stats(xml: lxml.etree.Element):
    for day in xml.find('UsageStats/UserActivity/loginActivity/edcLoginStats'):
        for user_login_timestamp in day.findall('userLoginTimestamps'):
            user_id = user_login_timestamp.find('userId').text
            for timestamp in user_login_timestamp.findall('loginTimestamp'):
                login_timestamp = datetime.datetime.strptime(timestamp.text, '%a %b %d %H:%M:%S %Z %Y')
                yield {'user_id': user_id, 'login_timestamp': login_timestamp}


def set_up_logging(settings: Settings):
    logging.basicConfig(format=settings.log_format, level='DEBUG', stream=sys.stdout)
    log.debug(f'get-catalog-statistics {settings.version}')
    if not settings.log_level == 'DEBUG':
        log.debug(f'Changing log level to {settings.log_level}')
    logging.getLogger().setLevel(settings.log_level)


def main_job(settings):
    db = settings.db
    data = get_raw_data(settings)
    for t in yield_login_stats(data):
        db.add_user_login_timestamp(**t)


def main():
    settings = Settings()
    set_up_logging(settings)

    log.info(f'RUN_AND_EXIT: {settings.run_and_exit}')
    main_job(settings)
    if not settings.run_and_exit:
        scheduler = apscheduler.schedulers.blocking.BlockingScheduler()
        scheduler.add_job(main_job, 'interval', hours=settings.sync_interval_hours, args=[settings])
        scheduler.start()


def handle_sigterm(_signal, _frame):
    sys.exit()


if __name__ == '__main__':
    signal.signal(signal.SIGTERM, handle_sigterm)
    main()
