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

def mailsender(html, msg, file_name_with_extension, path_of_the_file):
    sender_email = "fec.report.card@gmail.com"
    # receiver_email = "tech3@fecdirect.net"
    # receiver_email = "palashp@fecdirect.net"
    # receiver_email = "offersi@inboxopsteam.com"
    # receiver_email = ["tech3@fecdirect.net"]
    receiver_email = ["tech3@fecdirect.net"]
    # receiver_email_cc = "tech3@fecdirect.net"
    password = "rqoeirngqumuwsdt"

    message = MIMEMultipart("alternative")
    message["Subject"] = f"{msg} "
    message["From"] = f"Server mrc Report <fec.report.card@gmail.com>"
    if file_name_with_extension:
        message["To"] = ", ".join(receiver_email) #receiver_email
        # message["CC"] = "palashp@fecdirect.net" #receiver_email_cc
    else:
        message["To"] = "tech3@fecdirect.net"
    text = """\ Report """
    # Turn these into plain/html MIMEText objects
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part1)
    message.attach(part2)

    if file_name_with_extension:
        # open the file to be sent
        filename = file_name_with_extension
        attachment = open(path_of_the_file, "rb")

        # instance of MIMEBase and named as p
        p = MIMEBase('application', 'octet-stream')

        # To change the payload into encoded form
        p.set_payload((attachment).read())

        # encode into base64
        encoders.encode_base64(p)

        p.add_header('Content-Disposition', "attachment; filename= %s" % filename)

        # attach the instance 'p' to instance 'msg'
        message.attach(p)

    # Create secure connection with server and send email
    context1 = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context1) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, message.as_string())
    print("Mail sent")





server_nm = ("tt_disp_1","tt_00001","am-00146","rr-00052","rr-00051","rr-00053")
servers_info = f"""SELECT ssh_ip, server_mrc, server_name
FROM server
WHERE server_name NOT IN {server_nm}
  AND status != 'cancel'
  AND serv_type = 'small'
  AND use_purpose = 'mailing';
"""

servers_info_list = msa_db_session.execute(servers_info).fetchall()
result_dict= {}
sms_status_data = {}
username = "ibxglobal"
password = "veNa-eCha6c12_sms_069"
try:
    for (ip, cost, name) in servers_info_list:

        api_mrc_request = f"https://itryio.com/api/server?api_key=1e54d3b441cbbf8c86&ips[]={ip}&purchase=1"

        # Create a session to handle the authentication
        session = requests.Session()
        session.auth = (username, password)
        # Make the API request
        response_mrc = session.get(api_mrc_request)
        if response_mrc.status_code == 200:
            json_data_mrc = response_mrc.json()
            if json_data_mrc:
                # print(json_data)
                fees = json_data_mrc[0]['purchase']['fees']
                # print("*************************************************", ip, fees)
                delivery_date = json_data_mrc[0]['deliverydate']
                first_payment_fees = [fee for fee in fees if fee["type"] == "First Payment" and fee["item"] in ["Server", "Additional server fee"]]
                if fees:
                    first_payment = next(
                        (fee for fee in fees if fee["type"] == "First Payment"), None
                    )
                    one_time_fee = next(
                        (fee for fee in fees if fee["type"] == "One time fee"), None
                    )
                    if first_payment and one_time_fee and first_payment["payment_date"] == one_time_fee["payment_date"]:
                        # payment_date = first_payment["payment_date"]
                        one_time_fee_cost = Decimal(one_time_fee["cost"])
                        one_time_fee_next_due_date = one_time_fee["next_due_date"]
                        result_dict[ip] = {"cost": one_time_fee_cost, "next_due_date": one_time_fee_next_due_date}

                    elif first_payment_fees:
                        latest_fee = max(fees, key=lambda item: item['payment_date'])
                        latest_due_date = latest_fee["next_due_date"]
                        cost = Decimal(latest_fee["cost"])
                        result_dict[ip] = {"cost": cost, "next_due_date": latest_due_date}

                    else:
                        latest_fee = max(fees, key=lambda item: item['payment_date'])
                        latest_fee['cost'] = Decimal(latest_fee['cost'])
                        next_due_date = latest_fee['next_due_date']
                        payment_date = latest_fee['payment_date']
                        latest_fee_index = fees.index(latest_fee)
                        if len(fees) > 1:
                            latest_fee_index = fees.index(latest_fee)
                            if latest_fee_index > 0:
                                previous_fee = fees[latest_fee_index - 1]
                                previous_fee['cost'] = Decimal(previous_fee['cost'])
                                previous_payment_date = previous_fee['payment_date']
                            else:
                                previous_fee = None
                                previous_payment_date = None
                        else:
                            previous_fee = next(
                                (fee for fee in fees if fee['type'] == 'First Payment'), None
                            )
                            previous_payment_date = previous_fee['payment_date'] if previous_fee else None
                        result_dict[ip] = {
                            'delivery_date': delivery_date,
                            'last_billing_date': previous_payment_date,
                            'cost': latest_fee['cost'],
                            'next_due_date': next_due_date,
                        }
                else:
                    print("API request failed with status code:", response_mrc.text)


        else:
            print("API request failed with status code:", response_mrc.text)


    # print(len(sms_status_data))
    # print(sms_status_data)
    # for ip, values in result_dict.items():
    #     update_query = f"""
    #     UPDATE server
    #     SET
    #         in_date = '{values['delivery_date']}',
    #         last_billing_date = '{values['last_billing_date']}'
    #     WHERE ssh_ip = '{ip}';
    #     """
    #     msa_db_engine.execute(update_query)
    #     msa_db_session.commit()

    # print("successfully updated two fields(in_date, last_billing_date)")

    # for ip, status, server_name in


    changed_server_mrc = []
    red_mark_server_src = []
    for ip, cost, name in servers_info_list:
        ten_percent = Decimal('0.1')
        server_cost = result_dict.get(ip)
        if ip in result_dict:
            # Check if the cost has changed
            if cost != result_dict[ip]['cost'] or cost is None:
                changed_server_mrc.append((name, cost, result_dict[ip]['cost'], result_dict[ip]['next_due_date']))
            # Check if cost + 10% of cost is less than the previous cost
            if cost:
                if cost != result_dict[ip]['cost'] and cost + ten_percent * cost < result_dict[ip]['cost']:
                    # If the condition is satisfied, add the IP, current cost, and previous cost to the changed_ips list
                    red_mark_server_src.append((name, cost, result_dict[ip]['cost'], result_dict[ip]['next_due_date']))
    print(red_mark_server_src)
    list2_server_names = set(item[0] for item in red_mark_server_src)
    updated_changed_server_mrc = [item for item in changed_server_mrc if item[0] not in list2_server_names]

    html_part = f"""\
    <!DOCTYPE html>
    <html lang="en">
            <head>
            <meta charset="UTF-8">
            <meta http-equiv="X-UA-Compatible" content="IE=edge">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            </head>
    <body>
            <p>Hi Team,</p>
            <p>Please find server mrc report attached below</p>
            <p>Thanks and Regards,</p>
            <p>Tech3</p>
    </body>
    </html>
    """
    normal_server_data = []
    price_hike_server_data = []
    for name, current_cost, previous_cost, due_date in updated_changed_server_mrc:
        if name:
            # msa_db_session.query(Server).filter(Server.server_name == name).update({'server_mrc' : previous_cost})
            update_query = f"""UPDATE server
            SET server_mrc = {previous_cost}
            WHERE server_name = '{name}';
            """
            msa_db_engine.execute(update_query)
            msa_db_session.commit()
        print("Successfully updated db")
        normal_server_row = {}
        normal_server_row["Server Name"] = name
        normal_server_row["Last Month Cost(server table)"] = current_cost
        normal_server_row["Current Cost (api)"] = previous_cost
        normal_server_row["Renewable Date"] = due_date
        normal_server_data.append(normal_server_row)

    for name, current_cost, previous_cost, due_date in red_mark_server_src:
        if name:
            update_query = f"""UPDATE server
            SET server_mrc = {previous_cost}
            WHERE server_name = '{name}';
            """
            msa_db_engine.execute(update_query)
            msa_db_session.commit()
        print("Successfully updated db")
        hike_server_row = {}
        hike_server_row["Server Name"] = name
        hike_server_row["Last Month Cost(server table)"] = current_cost
        hike_server_row["Current Cost (api)"] = previous_cost
        hike_server_row["Renewable Date"] = due_date
        price_hike_server_data.append(hike_server_row)
    today_date = date.today()
    msg_part = f"Server MRC Report {today_date}"
    fields = ['Server Name', 'Last Month Cost(server table)', 'Current Cost (api)', 'Renewable Date']
    filename_normal = f"normal_server_mrc_report_{today_date}.csv"
    filename_hike = f"hike_server_mrc_report_{today_date}.csv"
    filepath = "/home/deploy/server_mrc/mrc_reports_file"
    def write_to_csv(data, filename):
        with open(filepath, 'w') as csvfile:
        # creating a csv dict writer object
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            writer.writerows(data)
        mailsender(html_part, msg_part, filename,filepath)
    if normal_server_data:
        write_to_csv(normal_server_data, filename_normal)
    if price_hike_server_data:
        write_to_csv(price_hike_server_data, filename_hike)




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
            <h1>Server mrc Report Not updated</h1>
            <p>Error: { str(e) }</p>
    </body>
    </html>
    """

    msg_part = ""

    try:
        mailsender(html=html_part, msg=msg_part, file_name_with_extension=None, path_of_the_file=None)
        pass
    except Exception as e:

        print('error while sending mail :', e)
