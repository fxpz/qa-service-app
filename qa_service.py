import sys
import json
import yaml
import argparse
import datetime
import requests

import gitlab
import psycopg2
import psycopg2.extras
import tornado.ioloop
import tornado.web
import tornado.httpserver

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
            web_branch_name varchar(100), \
            api_branch_name varchar(100));")
        conn.commit()
        cur.close()
    except:
        print >> sys.stderr, 'cannot initialize db'
        sys.exit(1)


class BaseHandler(tornado.web.RequestHandler):
    @tornado.web.asynchronous
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
        self.finish()


class GetApiBranchesHandler(BaseHandler):
    def get(self):
        auth_header = {'Authorization':
                       'token %s' % self._settings['GITHUB_TOKEN']}
        r = requests.get(self._settings['GITHUB_BR_API_URL'],
                         headers=auth_header)
        branches = [{'name': x['name'], 'value': x['name']} for x in r.json()]
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(branches))
        self.finish()


class GetPagetestsBranchesHandler(BaseHandler):
    def get(self):
        git = gitlab.Gitlab(self._settings['GITLAB_URL'],
                            token=self._settings['GITLAB_TOKEN'],
                            verify_ssl=self._settings['GITLAB_VERIFY_SSL'])
        r = git.getbranches(self._settings['GITLAB_PAGETESTS_PROJECT_ID'])
        branches = [{'name': x['name'], 'value': x['name']} for x in r]
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(branches))
        self.finish()


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
        self.finish()


class GetQaServerStatusHandler(BaseHandler):
    def get(self):
        conn = pg_connect(self._settings)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("SELECT * from qa_status ORDER BY last_update DESC")
        res = [['id', 'Server Name', 'Web Branch',
                'Api Branch', 'Last up', 'Status']]
        if cur.rowcount > 0:
            for row in cur.fetchall():
                res.append([row['qa_id'],
                            'qa-%s' % row['qa_id'],
                            row['web_branch_name'],
                            row['api_branch_name'],
                            row['last_update'].strftime('%Y-%m-%d %H:%M:%S'),
                            row['status']])
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(res))
        self.finish()


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
            self.finish()
        except:
            print >> sys.stderr, 'Error in clean query'
            self.write('Error executing cleanup query')
            self.finish()


class GetBranchNameByIdHandler(BaseHandler):
    def get(self, proj, qa_id):
        conn = pg_connect(self._settings)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        field_name = '%s_branch_name' % proj
        cur.execute('SELECT {field_name} FROM qa_status \
                    WHERE qa_id=%s'.format(field_name=field_name), (qa_id,))
        row = cur.fetchall()[0]
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode([{'name': row[field_name],
                                 'value': row[field_name]}]))
        self.finish()


class GetViralizePlaybookBranchHandler(BaseHandler):
    def get(self):
        res = []
        for branch in self._settings['VIRALIZE_PLAYBOOK_BRANCHES']:
            res.append({'name': branch, 'value': branch})
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode(res))
        self.finish()


class UpdateStatusHandler(BaseHandler):
    def post(self, qa_id):
        data = json.loads(self.request.body)
        data['last_update'] = datetime.datetime.now()
        conn = pg_connect(self._settings)
        cur = conn.cursor()
        try:
            update_query = ("UPDATE qa_status SET {fields} WHERE \
                            qa_id={qa_id}".format(
                qa_id=qa_id,
                fields=','.join(["%s='%s'" % (k, v) for
                                 k, v in data.iteritems()])))
            data['qa_id'] = qa_id
            insert_query = ("INSERT INTO qa_status ({fields}) SELECT {values} \
                            WHERE NOT EXISTS (SELECT 1 from qa_status WHERE \
                            qa_id={qa_id})").format(
                qa_id=qa_id,
                fields=','.join([k for k in data.iterkeys()]),
                values=','.join(["'%s'" % v for v in data.itervalues()]))

            cur.execute(update_query)
            cur.execute(insert_query)
            conn.commit()
            self.set_status(200)
            self.write('OK')
            self.finish()
        except:
            print >> sys.stderr, 'Error in update status query'
            self.set_status(500, reason='Error update status query')
            self.write('FAIL')
            self.finish()


class GetWebQaIdHandler(BaseHandler):
    def get(self, qa_id):
        self.set_header('Content-Type', 'application/json')
        self.write(json_encode([{'name': qa_id, 'value': qa_id}]))
        self.finish()


class GetMailUserHandler(BaseHandler):
    def get(self, user):
        for u in self._settings['USERS']:
            if u['user'] == user:
                self.write(u['mail'])
                break
        self.finish()


def make_app(settings):
    init_db(settings)
    return tornado.web.Application([
        (r"/web/get_qa_pr", GetWebQaPrHandler, dict(settings=settings)),
        (r"/api/get_branches", GetApiBranchesHandler, dict(settings=settings)),
        (r"/pagetests/get_branches",
         GetPagetestsBranchesHandler,
         dict(settings=settings)),
        (r"/get_qa_servers", GetQaServersHandler, dict(settings=settings)),
        (r"/get_qa_server_status",
         GetQaServerStatusHandler,
         dict(settings=settings)),
        (r"/clean_qa_server_status",
         CleanQaServerStatusHandler,
         dict(settings=settings)),
        (r"/get_branch_name_by_id/(web|api)/([0-9]+)",
         GetBranchNameByIdHandler,
         dict(settings=settings)),
        (r"/get_viralize_playbook_branch",
         GetViralizePlaybookBranchHandler,
         dict(settings=settings)),
        (r"/update_status/([0-9]+)",
         UpdateStatusHandler,
         dict(settings=settings)),
        (r"/web/get_qa_id/VR-([0-9]+)-.*",
         GetWebQaIdHandler,
         dict(settings=settings)),
        (r"/get_mail_user/(.*)",
         GetMailUserHandler,
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
    server = tornado.httpserver.HTTPServer(app)
    server.bind(settings['PORT'])
    server.start(settings['PROC_NUM'])
    tornado.ioloop.IOLoop.current().start()

if __name__ == '__main__':
    main()
