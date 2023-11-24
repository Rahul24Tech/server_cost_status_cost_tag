import requests
import xlsxwriter
import json
import csv
from sqlalchemy import create_engine
from urllib.parse import quote
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, Text, String, DateTime, Boolean, ForeignKey, UniqueConstraint, \
    PrimaryKeyConstraint, Index, text
from sqlalchemy.dialects.postgresql import UUID, ARRAY, JSONB, INET, CIDR, SMALLINT, NUMERIC, FLOAT
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import column_property
from sqlalchemy.schema import FetchedValue
from pydantic import BaseModel
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor

import copy
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from email.mime.base import MIMEBase
from datetime import datetime, date, timedelta
# from ..database.db_session import mailing_db_engine

import uuid
import pytz
from datetime import datetime
import random
import faker
from faker import Factory

from sqlalchemy import exc
from sqlalchemy.sql import func

from sqlalchemy.exc import SQLAlchemyError
import traceback
from sqlalchemy import and_, or_

pgdb_conx_details = {
    "mailing-db-pri": {
        "host": "10.160.0.6",
        "port": 5432,
        "db_user": "postgres",
        "db_passwd": "hYtR62@8Iomd;,(02",
        "ssl_mode": "disable",
        "db_name": "mailing"
    },
    "sponsor-db-pri": {
        "host": "10.160.0.6",
        "port": 5432,
        "db_user": "postgres",
        "db_passwd": "hYtR62@8Iomd;,(02",
        "ssl_mode": "disable",
        "db_name": "sponsor"
    },
    "msa-db-pri": {
        "host": "10.160.0.6",
        "port": 5432,
        "db_user": "postgres",
        "db_passwd": "hYtR62@8Iomd;,(02",
        "ssl_mode": "disable",
        "db_name": "msa"
    },
    "employee-db-pri": {
        "host": "10.160.0.6",
        "port": 5432,
        "db_user": "postgres",
        "db_passwd": "hYtR62@8Iomd;,(02",
        "ssl_mode": "disable",
        "db_name": "employee"
    },
    "oauth-db-pri": {
        "host": "10.160.0.6",
        "port": 5430,
        "db_user": "postgres",
        "db_passwd": "as87gvFa72fGuf2",
        "ssl_mode": "disable",
        "db_name": "oauth_db"
    }
}



database_uri = f'postgresql://' \
                           f'{pgdb_conx_details["msa-db-pri"]["db_user"]}:{quote(pgdb_conx_details["msa-db-pri"]["db_passwd"])}' \
                           f'@' \
                           f'{pgdb_conx_details["msa-db-pri"]["host"]}:{pgdb_conx_details["msa-db-pri"]["port"]}' \
                           f'/{pgdb_conx_details["msa-db-pri"]["db_name"]}?' \
                           f'sslmode={pgdb_conx_details["msa-db-pri"]["ssl_mode"]}'
msa_db_engine = create_engine(database_uri, echo=False, pool_timeout=5, pool_size=2, max_overflow=0, pool_pre_ping=True)
msa_db_session = sessionmaker(msa_db_engine)()



def mailsender_server_status(html, msg):
    sender_email = "fec.report.card@gmail.com"
    # receiver_email = "tech3@fecdirect.net"
    # receiver_email = "palashp@fecdirect.net"
    # receiver_email = "offersi@inboxopsteam.com"
    # receiver_email = ["tech3@fecdirect.net"]
    receiver_email = "tech3@fecdirect.net"
    # receiver_email_cc = "tech3@fecdirect.net"
    password = "rqoeirngqumuwsdt"

    message = MIMEMultipart("alternative")
    message["Subject"] = f"{msg} "
    message["From"] = f"SMS New Server Alert <fec.report.card@gmail.com>"

    text = """\ Report """
    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)



    # Create secure connection with server and send email
    context1 = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context1) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())
    print("Mail sent")



try:
    sms_status_data = {}
    username = "ibxglobal"
    password = "veNa-eCha6c12_sms_069"
    sms_ip_list = []
    sms_info = []
    result = []


    server_status_request = f"https://itryio.com/api/server?api_key=1e54d3b441cbbf8c86&statusm=New!&purchase=1"
    # Create a session to handle the authentication
    session = requests.Session()
    session.auth = (username, password)
    # Make the API request
    response_status = session.get(server_status_request)
    if response_status.status_code == 200:
        json_data_status = response_status.json()
        # json_data_status = [{'name': 'Nosahosting(193.193.203.153)', 'idserver': '518228', 'idgroupm': '448', 'alias': '', 'ip': '193.193.203.153', 'status': 'Ready for Production', 'status prod': 'New!', 'group': 'IBX24', 'affectdate': '2023-11-01 12:55:57', 'nextduedate': '2023-12-01', 'returndate': None, 'deliverydate': '2023-11-01 12:55:57', 'prevgroup': None, 'ASN': 'Lucky Net Ltd', 'geo': 'UA', 'usage': '', 'cancellation_date': None, 'purchase': {'id': '697998', 'name': 'Nosahosting20231028/000', 'status': 'Received', 'fees': [{'id': '2266771', 'type': 'First Payment', 'item': 'Server', 'cost': '110.000', 'payment_date': '2023-11-01', 'payment_type': 'OK', 'next_due_date': '2023-12-01'}]}}, {'name': 'Nosahosting(62.244.19.105)', 'idserver': '518229', 'idgroupm': '448', 'alias': '', 'ip': '62.244.19.105', 'status': 'Ready for Production', 'status prod': 'New!', 'group': 'IBX19', 'affectdate': '2023-11-01 12:50:16', 'nextduedate': '2023-12-01', 'returndate': None, 'deliverydate': '2023-11-01 12:50:16', 'prevgroup': None, 'ASN': 'Lucky Net Ltd', 'geo': 'UA', 'usage': '', 'cancellation_date': None, 'purchase': {'id': '697999', 'name': 'Nosahosting20231028/001', 'status': 'Received', 'fees': [{'id': '2266772', 'type': 'First Payment', 'item': 'Server', 'cost': '110.000', 'payment_date': '2023-11-01', 'payment_type': 'OK', 'next_due_date': '2023-12-01'}]}}]

        if json_data_status:
            for data in json_data_status:
                sms_status = data.get('status prod')
                sms_alias = data.get('alias')
                sms_ip = data.get('ip')
                sms_group = data.get('group')
                sms_usage = data.get('usage')

                if sms_status and sms_alias and sms_ip and sms_group in ['IBX23', 'IBX24', 'IBX25', 'IBX26'] and sms_usage == 'Mailing (Revenue)':
                    sms_ip_list.append(sms_ip)
                    sms_info.append((sms_status, sms_alias, sms_group, sms_usage, sms_ip))
    if sms_ip_list:
        for data in sms_ip_list:
            server_status = f"""SELECT server_name, ssh_ip
                                FROM server
                                WHERE ssh_ip = '{data}';
                            """
            servers_info_list_status = msa_db_session.execute(server_status).fetchall()

        if not servers_info_list_status:
            for (sms_status, sms_alias, sms_group, sms_usage, sms_ip) in sms_info:
                result.append((sms_status, sms_alias, sms_group, sms_usage, sms_ip))
        else:
            result = []
    if result:
        html_table = """
        <!DOCTYPE html>
        <html>
        <head>
        <style>
        table {
        border-collapse: collapse;
        width: 100%;
        max-width: 800px;
        margin: 0 auto;
        margin-top: 20px;
        }
        table, th, td {
        border: 1px solid black;
        }
        th, td {
        padding: 8px;
        text-align: left;
        }
        th {
        background-color: #f2f2f2;
        }
        @media screen and (max-width: 600px) {
        table {
            font-size: 14px;
        }
        }
        </style>
        </head>
        <body>
        <h3 style='color:green;'>Server with new status found in SMS but not in Portal!</h3>
        <table>
        <tr>
            <th>SMS Status</th>
            <th>SMS Alias</th>
            <th>SMS Group</th>
            <th>SMS usage</th>
            <th>SMS IP</th>
        </tr>
        """
        for row in result:
            html_table += f'<tr><td>{row[0]}</td><td>{row[1]}</td><td>{row[2]}</td><td>{row[3]}</td><td>{row[4]}</td></tr>'
        html_table += """
        </table>
        </body>
        </html>
        """
        # Print the HTML code
        # print(html_table)
        msg = "SMS New Server Alert"
        mailsender_server_status(html_table, msg)
    else:
        html_table = """
        <!DOCTYPE html>
        <html>
        <head>
        </head>
        <body>
        <h3 style='color:green;'>No mismatch found related to server status as New!</h3>
        </body>
        </html>
        """
        # Print the HTML code
        # print(html_table)
        msg = "SMS New Server Alert"
        mailsender_server_status(html_table, msg)

except Exception as e:
    error_str = f"Exception while generating report : {str(e)}"
    print(traceback.format_exc())

    html_part = f"""\
    <!DOCTYPE html>
    <html lang="en">
            <head>
            <meta charset="UTF-8">
            <meta http-equiv="X-UA-Compatible" content="IE=edge">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            </head>
    <body>
            <h1>SMS New Server Alert Not updated</h1>
            <p>Error: { str(e) }</p>
    </body>
    </html>
    """

    msg_part = ""

    try:
        mailsender_server_status(html=html_table, msg=msg_part)
        pass
    except Exception as e:

        print('error while sending mail :', e)