import sys
import yaml
import argparse
import requests

import psycopg2
import psycopg2.extras
import tornado.ioloop
import tornado.web

from tornado.escape import json_encode


def pg_connect(settings):
    try:
        conn = psycopg2.connect(
            dbname=settings['DB_NAME'],
            user=settings['DB_USER'],
            host=settings['DB_HOST'],
            password=settings['DB_PASS'],
            port=settings['DB_PORT'])
        return (conn)
    except:
        print >> sys.stderr, 'Could not connect to pgsql'
        sys.exit(1)


def init_db(settings):
    conn = pg_connect(settings)
    cur = conn.cursor()
    cur.execute("select table_name FROM information_schema.tables WHERE \
        table_catalog=%s and \
        table_schema='public';", (settings['DB_NAME'],))
    if cur.rowcount > 0:
        return(True)

    try:
        cur.execute("CREATE TABLE qa_status (\
            id serial PRIMARY KEY, \
            qa_id integer, \
            status varchar(50), \
            last_update timestamp, \
            branch_name varchar(100));")
        conn.commit()
        cur.close()
    except:
        print >> sys.stderr, 'cannot initialize db'
        sys.exit(1)


class BaseHandler(tornado.web.RequestHandler):
    def initialize(self, settings, *args, **kwargs):
        super(BaseHandler, self).initialize(*args, **kwargs)
        self._settings = settings


class GetWebQaPrHandler(BaseHandler):
    def get(self):
        auth_header = {'Authorization':
                       'token %s' % self._settings['GITHUB_TOKEN']}
        r = requests.get(self._settings['GITHUB_PR_SEARCH_URL'],
                         params=self._settings['GITHUB_PR_WEB_SEARCH_PARAMS'],
                         headers=auth_header)
        jres = r.json()
        pulls = [x['number'] for x in jres['items']]
        branches = []
        for pull in pulls:
            r = requests.get(self._settings['GITHUB_PR_WEB_URL'] % pull,
                             headers=auth_header)
            branches.append({"name": r.json()['head']['ref'],
                             "value": r.json()['head']['ref']})
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(branches))


class GetApiBranchesHandler(BaseHandler):
    def get(self):
        auth_header = {'Authorization':
                       'token %s' % self._settings['GITHUB_TOKEN']}
        r = requests.get(self._settings['GITHUB_BR_API_URL'],
                         headers=auth_header)
        branches = [{'name': x['name'], 'value': x['name']} for x in r.json()]
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(branches))


class GetQaServersHandler(BaseHandler):
    def get(self):
        import re
        import pyrax

        pyrax.set_setting('identity_type', 'rackspace')
        pyrax.set_credential_file(
            self._settings['RAX_CREDS_FILE'],
            region=self._settings['RAX_REGION'])
        cs = pyrax.cloudservers
        server_list = cs.servers.list()
        servers = []
        r = re.compile(r'qa-(\d+)$')
        for i in server_list:
            if r.match(i.name):
                servers.append({
                    'name': i.name,
                    'value': r.match(i.name).group(1)})
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(servers))


class GetQaServerStatusHandler(BaseHandler):
    def get(self):
        conn = pg_connect(self._settings)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * from qa_status ORDER BY last_update DESC")
        res = [['id', 'Server Name', 'Web Branch', 'Last up', 'Status']]
        if cur.rowcount > 0:
            for row in cur.fetchall():
                res.append([row['qa_id'],
                            'qa-%s' % row['qa_id'],
                            row['branch_name'],
                            row['last_update'].strftime('%Y-%m-%d %H:%M:%S'),
                            row['status']])
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(res))


class CleanQaServerStatusHandler(BaseHandler):
    def get(self):
        conn = pg_connect(self._settings)
        cur = conn.cursor()
        try:
            cur.execute("DELETE FROM qa_status WHERE status='Destroyed!' and "
                        "last_update < NOW() - INTERVAL '%s "
                        "days';", (self._settings['STATUS_RETENTION_DAYS'],))
            conn.commit()
            self.write('ok')
        except:
            print >> sys.stderr, 'Error in clean query'
            self.write('Error executing cleanup query')


class GetBranchNameByIdHandler(BaseHandler):
    def get(self, qa_id):
        conn = pg_connect(self._settings)
        cur = conn.cursor()
        cur.execute('SELECT branch_name FROM qa_status WHERE qa_id=%s',
                    (qa_id,))
        row = cur.fetchall()[0]
        self.write(row['branch_name'])


class GetViralizePlaybookBranchHandler(BaseHandler):
    def get(self):
        res = []
        for branch in self._settings['VIRALIZE_PLAYBOOK_BRANCHES']:
            res.append({'name': branch, 'value': branch})
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(res))


def make_app(settings):
    init_db(settings)
    return tornado.web.Application([
        (r"/web/get_qa_pr", GetWebQaPrHandler, dict(settings=settings)),
        (r"/api/get_branches", GetApiBranchesHandler, dict(settings=settings)),
        (r"/get_qa_servers", GetQaServersHandler, dict(settings=settings)),
        (r"/get_qa_server_status",
         GetQaServerStatusHandler,
         dict(settings=settings)),
        (r"/clean_qa_server_status",
         CleanQaServerStatusHandler,
         dict(settings=settings)),
        (r"/get_branch_name_by_id/([0-9]+)",
         GetBranchNameByIdHandler,
         dict(settings=settings)),
        (r"/get_viralize_playbook_branch",
         GetViralizePlaybookBranchHandler,
         dict(settings=settings))
    ])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-p', '--port', help='listen port',
                        default=8888, type=int)
    parser.add_argument('-c', '--conf', help='config file path',
                        default='./qa_service.yml')
    args = parser.parse_args()
    with open(args.conf) as f:
        settings = yaml.load(f)

    app = make_app(settings)
    app.listen(settings['port'])
    tornado.ioloop.IOLoop.current().start()

if __name__ == '__main__':
    main()
