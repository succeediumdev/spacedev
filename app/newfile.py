#''
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