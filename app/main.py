''
SPACE proxy server v2026.06.08
Itellectual property of Succeedium™ (https://succeedium.com)
Unauthorized use, distribution, transmission or publication strictly prohibited
'''
import urllib.request
import urllib.parse
import json
from fastapi import FastAPI, Header, Request, Response, Form
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.encoders import jsonable_encoder
import gzip
from io import BytesIO
import re
import base64
from typing import Annotated
import boto3
import botocore
from datetime import datetime
from psycopg2.extras import Json
from datetime import datetime, timedelta
import time
import os


from .utils.spaceLog import createLog


from .utils.validateAccessToken import validateAccessToken
from .utils.getAccessTokenName import getTokenName
from .utils.getTokenStatus import getTokenStatus

from .fetchLogs.transactions import transactions_router 
from .fetchLogs.audit import audit_router
from .fetchLogs.messages import message_router
from .python.pythonCfg import pythonCfgRouter
from .releases.releases import releases_router
from .releases.deploy import deploy_router
from .settings.verifyAccessToken import verifyAtRouter
from .settings.getTokens import getTokensRouter
from .settings.generateToken import generateTokenRouter
from .settings.deleteTokens import deleteTokensRouter
from .settings.createConnection import createConnectionRouter
from .settings.fetchConnections import fetchConnectionsRouter
from .settings.editConnection import editConnectionRouter
from .settings.deleteConnection import deleteConnectionRouter
from .compare.compareCubeData import compareCubeDataRouter
from .settings.getContent import getContentRouter
from .settings.jobs import jobsRouter
from .settings.testConnection import testConnectionRouter
from .modelSearch.refreshModelSearch import refreshModelSearchRouter
from .modelSearch.modelSearch import modelSearchRouter
from .modelSearch.modelSearchReplace import modelSearchReplaceRouter
from .python.jupyterRunPython import jupyterPythonRouter
from .python.jupyterEnvVariables import jupyterEnvRouter
from .settings.getUsage import getUsageRouter
from .settings.spaceLogs import spaceLogsRouter
from .email.Email import emailRouter
from .documentation.doc import docRouter
from .compare.compareRouter import compareRouter
from .settings.getTime import timeRouter
from .git.repositories import repositories_router
from .git.initialize import initialize_router
from .git.push import push_router
from .git.pull import pull_router
from .git.gitCompare import git_compare_router

from .utils.authenticateToken import authenticateToken
from .utils.readAccess import readAccess

from .db.postgres import init_postgres, get_postgres
from .db.insert import insertOne, insertMany
from .db.fetch import fetchOne

from .utils.env import env_variables

from .utils.getActiveUser import getActiveUser
from .utils.addCorsHeaders import addCorsHeaders
from .utils.pacloudproxy import pacloudproxy, pacloudtoken
from .utils.journal import getJournal, updateJournal
from .utils.snakeCase import snakeCase

from .utils.refreshCron import refreshCron

# POSTGRESQL QUERIES (move to other python file later)

init_postgres()
refreshCron()

postgres_conn = get_postgres()

create_table_query = """
CREATE TABLE IF NOT EXISTS python (
    name varchar(256) NOT NULL UNIQUE,
    updated_at timestamp,
    updated_by varchar(256),
    content text
);

CREATE TABLE IF NOT EXISTS doc (
    server varchar(256) NOT NULL UNIQUE,
    updated_at timestamp,
    updated_by varchar(256),
    refreshed_at timestamp,
    refreshed_by varchar(256),
    properties json,
    chart json
);

CREATE TABLE IF NOT EXISTS journal (
    table_name varchar(256) UNIQUE NOT NULL,
    updated_at timestamp,
    updated_by varchar(256),
    ts_first timestamp,
    ts_last timestamp
);

CREATE TABLE IF NOT EXISTS releases (
    release_name TEXT UNIQUE NOT NULL,
    description TEXT,
    server TEXT,
    updated_at timestamp,
    updated_by TEXT,
    log JSON,
    release_id SERIAL PRIMARY KEY,
    asset_order JSON
);

CREATE TABLE IF NOT EXISTS release_item (
    item_id SERIAL PRIMARY KEY,
    release_id INT REFERENCES releases(release_id) ON DELETE CASCADE,
    name TEXT,
    type TEXT,
    content JSON,
    updated_at TIMESTAMP,
    updated_by TEXT,
    url TEXT,
    container TEXT,
    server TEXT,
    manual TEXT
);

CREATE TABLE IF NOT EXISTS release_history (
    id SERIAL PRIMARY KEY,
    release_id INT REFERENCES releases(release_id) ON DELETE CASCADE,
    updated_by TEXT,
    updated_on TEXT,
    server TEXT,
    description TEXT,
    deployment_logs JSON
);

CREATE TABLE IF NOT EXISTS tokens (
    id SERIAL PRIMARY KEY,
    token TEXT,
    access JSON,
    email TEXT,
    status TEXT,
    name TEXT UNIQUE,
    expiry TIMESTAMP
);

CREATE TABLE IF NOT EXISTS connections (
    id SERIAL PRIMARY KEY,
    name TEXT,
    url TEXT,
    namespace TEXT,
    username TEXT,
    password TEXT,
    is_system BOOLEAN,
    type TEXT
);

CREATE TABLE IF NOT EXISTS data_tables (
    id SERIAL PRIMARY KEY,
    table_name TEXT UNIQUE,
    name TEXT,
    server TEXT
);

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT UNIQUE,
    description TEXT,
    type TEXT,
    schedule TEXT,
    schedule_description TEXT,
    enabled BOOLEAN,
    last_run TIMESTAMP,
    last_status TEXT,
    updated_at TIMESTAMP,
    updated_by TEXT,
    server TEXT,
    sync_interval TEXT,
    emails TEXT,
    file TEXT,
    args TEXT,
    success BOOLEAN,
    error BOOLEAN,
    retain NUMERIC,
    git_repo TEXT,
    git_action TEXT,
    job_details BOOLEAN
);

CREATE TABLE IF NOT EXISTS job_history (
    id SERIAL PRIMARY KEY,
    job_name TEXT,
    start_ts TIMESTAMP,
    end_ts TIMESTAMP,
    status TEXT,
    message TEXT
);

CREATE TABLE IF NOT EXISTS model_search_data (
    id SERIAL PRIMARY KEY,
    asset_type TEXT,
    data JSON,
    name TEXT,
    server TEXT
);

CREATE TABLE IF NOT EXISTS user_data (
    id SERIAL PRIMARY KEY,
    server TEXT,
    user_name TEXT,
    env_variables JSON
);

CREATE TABLE IF NOT EXISTS space_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    timestamp TIMESTAMP,
    type TEXT,
    scope TEXT,
    message TEXT,
    object TEXT,
    object_id TEXT
);

CREATE TABLE IF NOT EXISTS log_search (
    id SERIAL PRIMARY KEY,
    token TEXT,
    name TEXT,
    server TEXT,
    type TEXT,
    data JSON
);

CREATE TABLE IF NOT EXISTS git_repo (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE,
    url TEXT,
    branch TEXT,
    access_token TEXT,
    selected_server TEXT DEFAULT '',
    object_list JSON DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS invite_key (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key TEXT UNIQUE,
    token TEXT,
    host TEXT,
    env TEXT,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS git_compare (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    repo_id INT REFERENCES git_repo(id) ON DELETE CASCADE,
    comparison JSON
);

CREATE UNIQUE INDEX IF NOT EXISTS model_search_data_unique_idx
    ON model_search_data (name, asset_type, server);

"""

app = FastAPI()

debug = True

@app.exception_handler(Exception)
async def validation_exception_handler(request, err):
    try:
        base_error_message = f"Failed to execute: {request.method}: {request.url}"
        return JSONResponse(status_code=400, content={'message': f"{base_error_message}. Detail: {err}"})
    except:
        return JSONResponse(status_code=400, content={'message': f"An unexpected error has occured"})

def createServerTables(server, scope):
    txName = f"{snakeCase(server)}_transaction_log"
    auditName = f"{snakeCase(server)}_audit_log"
    messageName = f"{snakeCase(server)}_message_log"

    scopes = []
    if scope == "all":
        scopes = ["TransactionLogEntries", "AuditLogEntries", "MessageLogEntries"]
    else:
        scopes = [scope]

    query = ""
    params = []
    
    scopeNameMap = {
        "TransactionLogEntries": "Transaction log",
        "AuditLogEntries": "Audit log",
        "MessageLogEntries": "Message log"
    }

    if "TransactionLogEntries" in scopes:
        query += f"""
CREATE TABLE IF NOT EXISTS {txName} (
    id SERIAL PRIMARY KEY,
    ts timestamp,
    change_set_id TEXT,
    cube TEXT,
    replication_time timestamp,
    status_message TEXT,
    tuple TEXT,
    new_value TEXT,
    old_value TEXT,
    pa_user TEXT,
    CONSTRAINT unique_transaction_{txName} UNIQUE (
        ts, change_set_id, cube, status_message, tuple, new_value, old_value, pa_user
    )
);
"""
        # Add data_tables insert
        query += """
INSERT INTO data_tables (table_name, name, server) VALUES (%s, %s, %s) ON CONFLICT (table_name) DO NOTHING;
"""
        params.extend([txName, scopeNameMap["TransactionLogEntries"], server])

    if "AuditLogEntries" in scopes:
        query += f"""
CREATE TABLE IF NOT EXISTS {auditName} (
    id SERIAL PRIMARY KEY,
    description TEXT,
    object_name TEXT,
    object_type VARCHAR(256),
    ts TIMESTAMP,
    pa_user TEXT,
    details JSON,
    CONSTRAINT unique_audit_{auditName} UNIQUE (
        description, object_name, object_type, ts, pa_user
    )
);
"""
        # Add data_tables insert
        query += """
INSERT INTO data_tables (table_name, name, server) VALUES (%s, %s, %s) ON CONFLICT (table_name) DO NOTHING;
"""
        params.extend([auditName, scopeNameMap["AuditLogEntries"], server])

    if "MessageLogEntries" in scopes:
        query += f"""
CREATE TABLE IF NOT EXISTS {messageName} (
    id SERIAL PRIMARY KEY,
    level VARCHAR(256),
    logger TEXT,
    message TEXT,
    ts TIMESTAMP,
    CONSTRAINT unique_message_{messageName} UNIQUE (
        level, logger, message, ts
    )
);
"""
        # Add data_tables insert
        query += """
INSERT INTO data_tables (table_name, name, server) VALUES (%s, %s, %s) ON CONFLICT (table_name) DO NOTHING;
"""
        params.extend([messageName, scopeNameMap["MessageLogEntries"], server])

    try:
        with postgres_conn:
            with postgres_conn.cursor() as cursor:
                cursor.execute(query, params)
                postgres_conn.commit()
                cursor.close()
    except Exception as e:
        print("ERROR:", str(e))
        return
    
    if "TransactionLogEntries" in scopes:
        updateJournal(txName, None, None, "")
    if "AuditLogEntries" in scopes:
        updateJournal(auditName, None, None, "")
    if "MessageLogEntries" in scopes:
        updateJournal(messageName, None, None, "")

def insertMessageBackup(values, server, attempt=0):
    try:
        tableName = f"{snakeCase(server)}_message_log"
        query = f"""
    INSERT INTO {tableName}"""+""" (level, logger, message, ts)
    VALUES
        (%s, %s, %s, %s)
    ON CONFLICT (level, logger, message, ts)
    DO NOTHING;
    """
        insertingData = []

        lastTs = None
        firstTs = None

        for x in range(0, len(values)):
            value = values[x]
            output = (value["Level"] or "",
                    value["Logger"] or "",
                    value["Message"] or "",
                    value["TimeStamp"])
            try:
                ts = datetime.strptime(value["TimeStamp"], "%Y-%m-%dT%H:%M:%SZ")
            except:
                try:
                    ts = datetime.strptime(value["TimeStamp"], "%Y-%m-%dT%H:%MZ")
                except:
                    ts = datetime.strptime(value["TimeStamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
            
            if (lastTs is None or ts > lastTs):
                lastTs = ts
            if (firstTs is None or ts < firstTs):
                firstTs = ts

            insertingData.append(output)

        with postgres_conn:
            with postgres_conn.cursor() as cursor:
                cursor.executemany(query, insertingData)
                postgres_conn.commit()
                cursor.close()
        
        return [lastTs, firstTs]
    except:
        if attempt == 0:
            createServerTables(server, "MessageLogEntries")
            return insertMessageBackup(values, server, attempt=1)

def insertAuditBackup(values, server, attempt=0):
    try:
        tableName = f"{snakeCase(server)}_audit_log"
        query = f"""
    INSERT INTO {tableName}"""+""" (description, object_name, object_type, ts, pa_user, details)
    VALUES
        (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (description, object_name, object_type, ts, pa_user)
    DO NOTHING;
    """

        insertingData = []

        lastTs = None
        firstTs = None

        for x in range(0, len(values)):
            value = values[x]
            details = value.get("AuditDetails", [])
            output = (value["Description"] or "",
                    value["ObjectName"] or "",
                    value["ObjectType"] or "",
                    value["TimeStamp"],
                    value["UserName"],
                    Json(details))
            try:
                ts = datetime.strptime(value["TimeStamp"], "%Y-%m-%dT%H:%M:%SZ")
            except:
                ts = datetime.strptime(value["TimeStamp"], "%Y-%m-%dT%H:%MZ")
            
            if (lastTs is None or ts > lastTs):
                lastTs = ts
            if (firstTs is None or ts < firstTs):
                firstTs = ts

            insertingData.append(output)

        with postgres_conn:
            with postgres_conn.cursor() as cursor:
                cursor.executemany(query, insertingData)
                postgres_conn.commit()
                cursor.close()
        
        return [lastTs, firstTs]
    except:
        if attempt == 0:
            createServerTables(server, "AuditLogEntries")
            return insertAuditBackup(values, server, attempt=1)

def insertTxBackup(values, server, attempt=0):
    try:
        tableName = f"{snakeCase(server)}_transaction_log"
        query = f"""
    INSERT INTO {tableName}"""+""" (ts, change_set_id, cube, replication_time, status_message, tuple, new_value, old_value, pa_user)
    VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ts, change_set_id, cube, status_message, tuple, new_value, old_value, pa_user)
    DO NOTHING;
    """

        insertingData = []

        lastTs = None
        firstTs = None

        for x in range(0, len(values)):
            value = values[x]

            tupleVal = value["Tuple"]
            if tupleVal is not None and len(tupleVal) > 0:
                tupleVal = " · ".join(tupleVal)
            output = (value["TimeStamp"],
                value["ChangeSetID"] or "",
                value["Cube"] or "",
                value["ReplicationTime"],
                value["StatusMessage"] or "",
                tupleVal or "",
                value["NewValue"] or "",
                value["OldValue"] or "",
                value["User"] or "")
            try:
                ts = datetime.strptime(value["TimeStamp"], "%Y-%m-%dT%H:%M:%SZ")
            except:
                ts = datetime.strptime(value["TimeStamp"], "%Y-%m-%dT%H:%MZ")
            if (lastTs is None or ts > lastTs):
                lastTs = ts
            if (firstTs is None or ts < firstTs):
                firstTs = ts

            insertingData.append(output)

        with postgres_conn:
            with postgres_conn.cursor() as cursor:
                cursor.executemany(query, insertingData)
                postgres_conn.commit()
                cursor.close()
        
        return [lastTs, firstTs]
    except:
        if attempt == 0:
            createServerTables(server, "TransactionLogEntries")
            return insertTxBackup(values, server, attempt=1)

def cleanLogs(tableName, retain):
    check_table_query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = %s
    );
    """
    table_exists = fetchOne(check_table_query, (tableName,))
    
    if not table_exists or not table_exists[0]:
        return
        
    ts = datetime.now() - timedelta(days=retain*30)
    query = f"DELETE FROM {tableName} WHERE ts < %s;"
    insertOne(query, (ts,))

protectedPaths = {
    "/doc": "documentation",
    "/compare": "deployments",
    "/deploy": "deployments",
    "/release": "deployments",
    "/python": "python",
    "/config": "python",
    "/settings/connection/": "admin",
    "/settings/token": "admin",
    "/settings/content": "admin",
    "/settings/usage": "admin",
    "/settings/job": "admin",
    "/settings/logs": "admin",
    "/logs/messages": "messagelog",
    "/logs/transactions": "transactionlog",
    "/logs/audit": "auditlog",
    "/git": "git"
}


@app.middleware("http")
async def token_middleware(request: Request, call_next):
    path = request.url.path
    response = await call_next(request)

    if path == "/refresh-cookies":
        return await authenticateToken(request, response, "", update=True)

    matchedPath = ""

    for key in protectedPaths:
        if path.startswith(key):
            matchedPath = key
            break
    
    pathProtection = protectedPaths.get(matchedPath, "")
    return await authenticateToken(request, response, pathProtection)


app.include_router(transactions_router)
app.include_router(audit_router)
app.include_router(message_router)
app.include_router(pythonCfgRouter)
app.include_router(releases_router)
app.include_router(deploy_router)
app.include_router(verifyAtRouter)
app.include_router(getTokensRouter)
app.include_router(generateTokenRouter)
app.include_router(deleteTokensRouter)
app.include_router(createConnectionRouter)
app.include_router(fetchConnectionsRouter)
app.include_router(editConnectionRouter)
app.include_router(deleteConnectionRouter)
app.include_router(compareCubeDataRouter)
app.include_router(getContentRouter)
app.include_router(jobsRouter)
app.include_router(testConnectionRouter)
app.include_router(refreshModelSearchRouter)
app.include_router(modelSearchRouter)
app.include_router(modelSearchReplaceRouter)
app.include_router(jupyterPythonRouter)
app.include_router(jupyterEnvRouter)
app.include_router(getUsageRouter)
app.include_router(spaceLogsRouter)
app.include_router(emailRouter)
app.include_router(docRouter)
app.include_router(compareRouter)
app.include_router(timeRouter)
app.include_router(repositories_router)
app.include_router(initialize_router)
app.include_router(push_router)
app.include_router(pull_router)
app.include_router(git_compare_router)

@app.get("/proxy")
async def get_proxy_2(request: Request = {}):
    query = request.query_params
    b64urlquery = query.get("url")

    b64urlclean = "=".join(b64urlquery.split("-"))
    url = base64.b64decode(b64urlclean).decode("utf-8")

    return await pacloudproxy(request, url)

@app.post("/proxy")
async def post_proxy_2(request: Request = {}):
    query = request.query_params
    b64urlquery = query.get("url")

    b64urlclean = "=".join(b64urlquery.split("-"))
    url = base64.b64decode(b64urlclean).decode("utf-8")

    return await pacloudproxy(request, url)

@app.options("/")
async def options_proxy(url: str = "", request: Request = {}):
    return JSONResponse(content={'message': "SPACE preflight handler"}, headers=addCorsHeaders({}, (request.headers.get('Referer') or '').strip('/') or '*'))

@app.get("/")
async def get_proxy(url: str = "", request: Request = {}):
    if url=="":
        return JSONResponse(content={'message': "SPACE is up!"}, headers=addCorsHeaders({}, (request.headers.get('Referer') or '').strip('/') or '*'))
    else:
        return await pacloudproxy(request, url)

@app.post("/")
async def post_proxy(url: str = "", request: Request = {}):
    return await pacloudproxy(request, url)

@app.patch("/")
async def patch_proxy(url: str = "", request: Request = {}):
    return await pacloudproxy(request, url)

@app.delete("/")
async def delete_proxy(url: str = "", request: Request = {}):
    return await pacloudproxy(request, url)

@app.put("/")
async def put_proxy(url: str = "", request: Request = {}):
    return await pacloudproxy(request, url)

@app.options("/oauth/token/")
async def options_oauth_token(code: str = "", request: Request = {}):
    return JSONResponse(content={'message': "/oauth/token/ TeamOne preflight handler"}, headers=addCorsHeaders({}, (request.headers.get('Referer') or '*').strip("/")))

@app.get("/oauth/token/")
async def get_oauth_token(code: str = "", request: Request = {}):
    payload = 'grant_type=authorization_code&code='+urllib.parse.quote(code.replace(' ', '+'), safe='')
    return await pacloudtoken(request, payload)

@app.options("/oauth/login/")
async def options_oauth_login(code: str = "", request: Request = {}):
    return JSONResponse(content={'message': "/oauth/login/ TeamOne preflight handler"}, headers=addCorsHeaders({}, (request.headers.get('Referer') or '*').strip("/")))

@app.get("/oauth/login/")
async def get_oauth_login(t: str = "", request: Request = {}):
    # Refresh access_token if refresh_token is provided, otherwise redirect to the SSO
    if t=="":
        host = os.getenv("PA_HOST").replace("https://", "")
        # Use PROXY_HOST!!! Cannot get proxy host from request: proxy = str(request.url).split('/oauth/login')[0].replace("https://", "").replace("http://", "")
        proxy = os.getenv("SPACE_HOST").replace("https://", "")
        return Response(media_type='text/html', headers=addCorsHeaders({},(request.headers.get('Referer') or '*').strip("/")), content=f"<html><head><script>location.href='https://{host}/oauth2/auth?response_type=code&client_id={env_variables['PA_CLIENT_ID']}&scope=tm1Context&state=TEAMONE&redirect_uri=https://{proxy}/oauth/token';</script></head><body/></html>")
    else:
        payload = 'grant_type=refresh_token&refresh_token='+urllib.parse.quote(base64.b64decode(t).decode().replace(' ', '+'), safe='')
        return await pacloudtoken(request, payload)
    
@app.options("/space/")
async def options_space(url: str = "", request: Request = {}):
    return JSONResponse(content={'message': "/space/ preflight handler"}, headers=addCorsHeaders({}, (request.headers.get('Referer') or '*').strip("/")))

@app.get("/space/")
async def get_space(url: str = "", request: Request = {}):
    if url=="":
        return JSONResponse(content={'message': "SPACE is working!"}, headers=addCorsHeaders({}, request.headers.get('Referer') or '*'))
    headers = False
    urlList = url.split('api/v1/')
    endpoint = urlList[1].split('?')[0]
    response = await pacloudproxy(request, urlList[0]+'api/v1/ActiveUser',headers)
    if(response.status_code!=200):
        return response
    user = json.loads(response.body)

    # Validate url
    if os.getenv("SPACE_AUTH_URL"):
        if endpoint not in os.getenv("SPACE_AUTH_URL").split('|'):
            return JSONResponse(status_code=401, content={'message': 'Unauthorized PA endpoint: '+urlList[1].split('?')[0]}, headers=addCorsHeaders({}, (request.headers.get('Referer') or '*').strip("/")))

    # Current user filter for non-admin users
    if not user['Type'] == 'Admin':
        if endpoint in ['TransactionLogEntries','AuditLogEntries']:
            url += ("&" if '?' in url else '') + f"User eq '{user['FriendlyName']}'"
        elif endpoint == 'MessageLogEntries':
            #TODO: filter MessageLogEntries https://tm1.succeedium.com:52670/api/v1/Threads?$expand=Session($select=ID)
            url += ''

    if endpoint == "AuditLogEntries":
        url += ("&" if '?' in url else '') + f"$expand=AuditDetails($select=Description)"

    # Fetch url using admin AUTH_HEADER
    return await pacloudproxy(request, url, {'Authorization': os.getenv("SPACE_AUTH_HEADER"), 'TM1-SessionContext': 'SPACE', 'accept':'*/*','content-type':'application/json'})

@app.post("/space/")
async def post_space(request: Request = {}, response: Response = {}):
    referer = (request.headers.get('Referer') or '*').strip("/")
    
    paLocal = request.headers.get("PALocal", "")

    accessToken = request.headers.get("space-token")
    accessArr = readAccess(accessToken)

    print("\nSPACE\n")


    try:
        r = await request.json()
    except ValueError as e:
        return JSONResponse(status_code=400, content={'message': 'Invalid JSON in request body'}, headers=addCorsHeaders({}, referer, response))
    action = (r.get('action') or '')
    if action == '':
        return JSONResponse(status_code=400, content={'message': "Action is missing"}, headers=addCorsHeaders({}, referer, response))
    # Use secret if provided, otherwise validate active user is admin
    secret = r.get('secret') or ''
    if not secret == os.getenv("SPACE_SECRET"):
        headers = {'Authorization': request.headers.get('Authorization') or '', 'TM1-SessionContext': 'SPACE', 'accept':'*/*;odata.metadata=none','content-type':'application/json'}
        user = await getActiveUser(request, response)
        
        if user is None:
            return JSONResponse(status_code=400, content={'message': "An unexpected error has occured"}, headers=addCorsHeaders({}, referer, response))
        if not user['Type'] == 'Admin':
            return JSONResponse(status_code=401, content={'action': action, 'message': f"Unauthorized! {user['FriendlyName']} is not ADMIN in {env_variables['PA_SERVER']}"}, headers=addCorsHeaders({}, referer, response))
        if action in ['EXPORT','IMPORT']:
            return JSONResponse(status_code=401, content={'action': action, 'message': 'Unauthorized SPACE admin!'}, headers=addCorsHeaders({}, referer, response))
    try:
        session = boto3.Session(region_name = os.getenv("S3_AWS_REGION"), aws_access_key_id = os.getenv("S3_AWS_ACCESS_KEY_ID"), aws_secret_access_key = os.getenv("S3_AWS_SECRET_ACCESS_KEY"))
        s3 = session.resource('s3')
    except Exception as error:
        return error
    if action in ['BACKUP','RECYCLE']:
        scope = r.get('scope') or None
        server = r.get('server') or 'NO_SERVER_SPECIFIED'
        if scope not in ['TransactionLogEntries','AuditLogEntries','MessageLogEntries']:
            return JSONResponse(status_code=400, content={'action': action, 'message': f"Invalid scope: {scope}. Expected: TransactionLogEntries or AuditLogEntries or MessageLogEntries"}, headers=addCorsHeaders({}, referer, response))
        
        createServerTables(server, scope)

        if action == 'BACKUP':
            journal = getJournal(server, scope)
            if journal is None:
                return JSONResponse(status_code=400, content={'action': action, 'message': f"An unexpected error has occured"}, headers=addCorsHeaders({}, referer, response))

            periodFrom = r.get('periodFrom') or None
            periodTo = r.get('periodTo') or None
            if periodTo is None:
                periodTo = datetime.now().strftime("%Y-%m-%d")

            if (periodFrom is None and journal is not None and journal["ts_last"] is not None):
                periodFrom = journal["ts_last"].strftime("%Y-%m-%dT%H:%M:%SZ")

            if periodFrom is None:
                yesterday = datetime.now() - timedelta(days=30)
                periodFrom = yesterday.strftime("%Y-%m-%d")
                periodFrom = f"{periodFrom}T00:00:00Z"

            skipRecords = int(r.get("skipRecords") or 0)
            topRecords = int(os.getenv("SPACE_BATCH_SIZE"))

            # Loop entries and save them to SPACE
            odataCount = 0
            firstReq = True
            headers = {'Authorization': request.headers.get('Authorization') or '', 'TM1-SessionContext': 'SPACE', 'accept':'*/*;odata.metadata=none','content-type':'application/json'}
            lastTs = None
            firstTs = None

            updated = False

            while (firstReq or skipRecords + topRecords < odataCount):
                if (not firstReq):
                    skipRecords += topRecords

                # Generates url
                tsFilter = urllib.parse.quote(f"TimeStamp gt {periodFrom} and TimeStamp lt {periodTo}T23:59:59Z")
                
                serverPrefix = f"https://{env_variables['PA_HOST']}/tm1/api/{server}/api/v1/"
                if paLocal != "":
                    serverPrefix = paLocal

                url = f"{serverPrefix}{scope}?$skip={skipRecords}&$top={topRecords}&$filter={tsFilter}&$orderby=TimeStamp%20desc&$count=true"
                if scope == "AuditLogEntries":
                    url += "&$expand=AuditDetails($select=Description)"
                res = await pacloudproxy({'method':'GET'}, url, headers,'GET',referer)

                odata = json.loads(res.body)

                values = odata.get("value", [])

                # Inserts in database
                if scope == "TransactionLogEntries":
                    [lts, fts] = insertTxBackup(values, server)
                    updated = True
                elif scope == "AuditLogEntries":
                    [lts, fts] = insertAuditBackup(values, server)
                    updated = True
                elif scope == "MessageLogEntries":
                    [lts, fts] = insertMessageBackup(values, server)
                    updated = True

                if lts is not None:
                    if lastTs is None:
                        lastTs = lts
                    elif lts > lastTs:
                        lastTs = lts
                if fts is not None:
                    if firstTs is None:
                        firstTs = fts
                    elif fts < firstTs:
                        firstTs = fts

                firstReq = False
                prevOdata = odataCount
                odataCount = odata.get("@odata.count")
                if odataCount is None:
                    break

            # UPDATE JOURNAL
            if odataCount is None:
                odataCount = prevOdata

            contentName = ""
            if scope == "TransactionLogEntries":
                contentName = "Transaction log"
            elif scope == "AuditLogEntries":
                contentName = "Audit log"
            elif scope == "MessageLogEntries":
                contentName = "Message log"

            if updated:
                if scope == "TransactionLogEntries":
                    tableName = f"{snakeCase(server)}_transaction_log"
                elif scope == "AuditLogEntries":
                    tableName = f"{snakeCase(server)}_audit_log"
                elif scope == "MessageLogEntries":
                    tableName = f"{snakeCase(server)}_message_log"

                updateJournal(tableName, lastTs, firstTs, user["FriendlyName"])

            retain = r.get('retain', 0)
            if retain:
                cleanLogs(tableName, retain)

            spaceToken = request.headers.get("space-token", "")
            userName = getTokenName(spaceToken)

            parsedFrom = datetime.strptime(periodFrom.split("T")[0], "%Y-%m-%d").strftime("%Y-%m-%d")
            parsedTo = datetime.strptime(periodTo.split("T")[0], "%Y-%m-%d").strftime("%Y-%m-%d")
            logObject = {
                "scope": scope,
                "server": server,
                "periodFrom": parsedFrom,
                "periodTo": parsedTo
            }

            createLog("Info", "Logs", f"{userName} updated {server} > {contentName}: {json.dumps(logObject)}", "Logs")
            res = {"message": "Backup successfull!"}
        else:
            # Recycle entries
            url =f'DELETE FROM {scope} WHERE Timestamp BETWEEN {periodFrom} AND {periodTo}'
    elif action == 'INIT':
        # Precreate a simply table in PostgreSQL
        try:
            with postgres_conn:
                msg = ""
                # Displays message if tables exist
                with postgres_conn.cursor() as cursor:
                    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                    tableNames = cursor.fetchall()
                    if (len(tableNames) == 0):
                        msg = "Tables created"
                    else:
                        msg = "Tables already exist"
                    cursor.close()
                # Creates tables
                with postgres_conn.cursor() as cursor:
                    cursor.execute(create_table_query)
                    postgres_conn.commit()
                    cursor.close()
                res = {'message':msg}
        except Exception as e:
            res = {"message": "An unexpected error has occured"}

    return JSONResponse(content=res, headers=addCorsHeaders({}, (request.headers.get('Referer') or '*').strip("/"), response))

@app.get("/welcome/succeedium-planning-analytics-cloud-extension-auth")
async def authReq(request: Request = {}, response: Response = {}, key="", confirm=False):
    inviteKeyDataQuery = """SELECT * FROM invite_key WHERE key = %s AND active = TRUE"""
    inviteKeyData = fetchOne(inviteKeyDataQuery, (key,))

    if inviteKeyData is None:
        return HTMLResponse(content="<p>The invite you used is invalid.</p>")
    
    token = inviteKeyData[2]
    host = inviteKeyData[3]
    env = inviteKeyData[4]

    token_check_query = """SELECT id FROM tokens WHERE token = %s"""
    existing_token = fetchOne(token_check_query, (token,))

    if existing_token is None:
        return HTMLResponse(content="<p>The invite you used is invalid.</p>")
    

    if confirm:
        update_query = """UPDATE invite_key SET active = FALSE WHERE key = %s"""
        insertOne(update_query, (key,))

        return Response(status_code=302, headers={"Location": f"https://{host}"})

    jsonDump = json.dumps({"token": token, "host": host, "env": env})

    return HTMLResponse(content=f"<p>Redirecting...<br>If you don't get redirected, install SPACE.</p><span id='json' style='display: none;'>{jsonDump}</span>")

@app.get("/version")
async def get_version(request: Request = {}, response: Response = {}):
    version = os.getenv("SPACE_VERSION", "")

    spaceToken = request.headers.get("space-token", "")

    tokenStatus = getTokenStatus(spaceToken)

    return {"version": version, "tokenStatus": tokenStatus}

@app.post("/refresh-cookies")
async def refresh_cookies(request: Request = {}, response: Response = {}):
    referer = (request.headers.get('Referer') or '*').strip("/")

    await getActiveUser(request, response, update=True)

    return JSONResponse(status_code=200, content={'message': f"Refreshed"}, headers=addCorsHeaders({}, referer, response))

@app.post("/space/testconnection")
async def test_connection(request: Request = {}, response: Response = {}):
    referer = (request.headers.get('Referer') or '*').strip("/")

    body = await request.json()

    host = body.get("host", "")
    token = body.get("testToken", "")

    validToken = validateAccessToken(token)

    if not validToken:
        return JSONResponse(status_code=400, content={'message': f"Invalid token", "status": "error"}, headers=addCorsHeaders({}, referer, response))

    return JSONResponse(status_code=200, content={'message': f"Connected successfully", "status": "success"}, headers=addCorsHeaders({}, referer, response))