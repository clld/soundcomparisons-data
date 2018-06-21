from sqlalchemy import create_engine


class DB(object):
    def __init__(self, db='soundcomparisons', user='soundcomparisons', password='pwd'):
        self.engine = create_engine('mysql+pymysql://{0}:{1}@localhost/{2}?charset=utf8mb4'.format(
            user, password, db))

    def __call__(self, *args, **kw):
        return self.engine.execute(*args, **kw)
