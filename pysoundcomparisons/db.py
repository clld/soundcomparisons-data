from sqlalchemy import create_engine


class DB(object):
    def __init__(self, host='localhost', db='soundcomparisons', user='soundcomparisons', password='pwd'):
        self.engine = create_engine('mysql+pymysql://{1}:{2}@{4}/{3}?charset=utf8mb4'.format(
            host, user, password, db))

    def __call__(self, *args, **kw):
        return self.engine.execute(*args, **kw)
