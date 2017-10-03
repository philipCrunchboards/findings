from io import StringIO
import datetime
import falcon
import json

from json import JSONEncoder

import os
import psycopg2cffi.pool
import dj_database_url
from wsgi_basic_auth import BasicAuth
from psycopg2cffi.extras import Json

database_config = dj_database_url.config()

pool = psycopg2cffi.pool.ThreadedConnectionPool(1, 10,
                                                host=database_config['HOST'],
                                                user=database_config['USER'],
                                                port=5432,
                                                password=database_config['PASSWORD'],
                                                database=database_config['NAME'])


class DateAndDateTimeSupportingJSONEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, (datetime.date, datetime.datetime)):
            return o.isoformat()

        return super(DateAndDateTimeSupportingJSONEncoder, self).default(o)


_default_encoder_class = DateAndDateTimeSupportingJSONEncoder
_default_encoder = _default_encoder_class()


def json_stream(obj, skipkeys=False, ensure_ascii=True, check_circular=True,
                allow_nan=True, cls=None, indent=None, separators=None,
                encoding='utf-8', default=None, sort_keys=False, **kw):
    """Serialize ``obj`` and yield chunks."""
    # cached encoder
    if (not skipkeys and ensure_ascii and
            check_circular and allow_nan and
                cls is None and indent is None and separators is None and
                encoding == 'utf-8' and default is None and not sort_keys and not kw):
        iterable = _default_encoder.iterencode(obj)
    else:
        if cls is None:
            cls = _default_encoder_class
        iterable = cls(skipkeys=skipkeys, ensure_ascii=ensure_ascii,
                       check_circular=check_circular, allow_nan=allow_nan, indent=indent,
                       separators=separators, encoding=encoding,
                       default=default, sort_keys=sort_keys, **kw).iterencode(obj)

    for chunk in iterable:
        yield chunk

def buffered(iterable, buflen=4096):
    buffer = None
    seen = 0
    for chunk in iterable:
        if buffer is None:
            buffer = StringIO.StringIO()
        buffer.write(chunk)
        seen += len(chunk)
        if seen > buflen:
            buffer.seek(0)
            yield buffer.read()
            buffer = None
            seen = 0
    if buffer is not None:
        buffer.seek(0)
        yield buffer.read()

class JSONTranslator(object):
    def process_request(self, req, resp):
        # req.stream corresponds to the WSGI wsgi.input environ variable,
        # and allows you to read bytes from the request body.
        #
        # See also: PEP 3333
        if req.content_length in (None, 0):
            # Nothing to do
            return

        try:
            req.context['doc'] = json.load(req.stream)

        except (ValueError, UnicodeDecodeError):
            raise falcon.HTTPError(falcon.HTTP_753,
                                   'Malformed JSON',
                                   'Could not decode the request body. The '
                                   'JSON was incorrect or not encoded as '
                                   'UTF-8.')

    def process_response(self, req, resp, resource):
        if 'result' not in req.context:
            return
        resp.stream = buffered(json_stream(req.context['result']))


class PostgresConnectionPool(object):
    def process_request(self, req, resp):
        req.context['connection'] = pool.getconn()

    def process_response(self, req, resp, resource):
        if 'connection' not in req.context:
            return

        pool.putconn(req.context['connection'])


class MatchResource:
    """
    create table matches (
      date DATE PRIMARY KEY,
      success BIGINT DEFAULT 0,
      failure BIGINT DEFAULT 0
    );
    """

    def on_post(self, req, resp):
        doc = req.context['doc']
        today = datetime.date.today()
        with req.context['connection'] as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO matches (date, success, failure) "
                        "VALUES (%s, (%s = TRUE)::INTEGER, (%s = FALSE)::INTEGER) "
                        "ON CONFLICT (date) DO UPDATE SET "
                        "success = matches.success + (%s = TRUE)::INTEGER, "
                        "failure = matches.failure + (%s = FALSE)::INTEGER "
                        "WHERE matches.date = %s",
                        [today, doc, doc, doc, doc, today])


class MismatchDataResource:
    """
    create table failures (
      id BIGSERIAL PRIMARY KEY,
      data json
    );
    """

    def on_post(self, req, resp):
        name = req.params.get('name', 'unnamed')
        doc = req.context['doc']
        with req.context['connection'] as conn:
            cur = conn.cursor()
            cur.execute("INSERT INTO failures (name, data) VALUES (%s, %s)", [name, Json(doc)])


class StreamedDict(dict):
    def __init__(self, source, f=lambda x: x):
        self.source = source
        self.f = f

    def iteritems(self):
        for x in self.source:
            yield self.f(x)

    def __len__(self):
        return 1


class StreamedList(list):
    def __init__(self, source, f=lambda x: x):
        self.source = source
        self.f = f

    def __iter__(self):
        for x in self.source:
            yield self.f(x)

    def __len__(self):
        return 1


class DataDownloadResource:
    """
    Big JSON document, with all your datas.
    """

    def on_get(self, req, resp):
        with req.context['connection'] as conn:
            match_cur = conn.cursor()
            match_cur.execute("SELECT * FROM matches")

            def date_match_record(record):
                return record[0].isoformat(), {
                    'success': record[1],
                    'failure': record[2],
                }

            failure_cur = conn.cursor()
            failure_cur.execute("SELECT * FROM failures")
            resp.content_type = 'application/json'
            req.context['result'] = {
                'stats': StreamedDict(match_cur, date_match_record),
                'failures': list(failure_cur)
            }


app = falcon.API(middleware=[
    JSONTranslator(),
    PostgresConnectionPool(),
])

match = MatchResource()
mismatch_data = MismatchDataResource()

app.add_route('/match/', match)
app.add_route('/mismatch-data/', mismatch_data)
app.add_route('/get-match-data/', DataDownloadResource())

if os.getenv('WSGI_AUTH_CREDENTIALS'):
    app = BasicAuth(app)
