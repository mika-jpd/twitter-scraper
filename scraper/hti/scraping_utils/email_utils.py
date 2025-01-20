import requests
import json
import os
import pickle
import re
import asyncio
import imaplib
from imap_tools import MailBox
from itertools import chain

# Gmail API logistical_utils
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import base64
from typing import Callable, Any, TypeVar, Iterable

SCOPES = ['https://mail.google.com/']
our_email = 'flumpy180600@gmail.com'


def gmail_authenticate():
    # if it's there it's there if not good luck lmao
    path_gmail_token = os.environ.get('PATH_GMAIL_TOKEN')
    creds = None
    # the file token_.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time
    if os.path.exists(os.path.join(path_gmail_token, "token.pickle")):
        with open(os.path.join(path_gmail_token, "token.pickle"), "rb") as token:
            creds = pickle.load(token)

    # if there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                os.path.join(
                    path_gmail_token, 'flumpy_gmail_credentials.json'
                ), SCOPES)
            creds = flow.run_local_server(port=0)
        # save the credentials for the next run
        with open(os.path.join(path_gmail_token, "token.pickle"), "wb") as token:
            pickle.dump(creds, token)
    return build('gmail', 'v1', credentials=creds)


def search_messages(service, query):
    result = service.users().messages().list(userId='me', q=query).execute()
    messages = []
    if 'messages' in result:
        messages.extend(result['messages'])
    while 'nextPageToken' in result:
        page_token = result['nextPageToken']
        result = service.users().messages().list(userId='me', q=query, pageToken=page_token).execute()
        if 'messages' in result:
            messages.extend(result['messages'])
    return messages


def extract_authentication_code(service, message: str):
    """
    #Todo: how to find the code within the message body ?
    """
    msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()
    # parts can be the message body, or attachments
    payload = msg['payload']
    headers = payload.get("headers")
    confirmation_code = next(
        (h for h in headers if h['name'] == 'Subject'), None
    )
    if confirmation_code:
        subject = confirmation_code['value']
        val = re.findall(r'^\d+', subject)
        if len(val) > 1:
            raise Exception(f"Wtf len(val) == {len(val)} but it's supposed to be 1...")
        elif len(val) < 1:
            val = [subject.strip("Your X confirmation code is ")]
        return val[0]
    else:
        return None

async def get_authentication_code_imap(email_address: str, password: str) -> str:
    imap_ssl_host = 'imap.firstmail.ltd'
    imap_ssl_port = 993
    username = email_address
    password = password

    await asyncio.sleep(5)

    messages = []
    # Get date, subject and body len of all emails from INBOX folder
    with MailBox(imap_ssl_host).login(username, password) as mailbox:
        for msg in mailbox.fetch():
            messages.append(msg)

    # get the ones with verification code
    messages = [msg for msg in messages if "@x.com" in msg.from_]

    # get the latest one
    verification_message = max(messages, key=lambda msg: msg.date)
    if "verification" in verification_message.subject:
        verification_code = extract_verification_code_imap(verification_message.subject)
    else:
        verification_code = extract_confirmation_code_imap_new(verification_message.subject)

    return verification_code

async def get_authentication_code(email_address: str, service=None) -> str:
    if service is None:
        service = gmail_authenticate()
    # after authentication
    # search for the confirmation email
    query = f"from:info_at_x.com_{email_address}"
    code = None
    for i in range(10):
        emails = search_messages(service=service, query=query)
        if code:
            break
        if len(emails) > 0:
            last_email = emails[0]
            code = extract_authentication_code(service=service, message=last_email)
        else:
            await asyncio.sleep(5)
    if not code:
        raise ValueError("No verification code found")
    else:
        return code


def extract_verification_code(service, message: str):
    msg = service.users().messages().get(userId='me', id=message['id'], format='full').execute()

    # find it in the snippet
    snippet = msg["snippet"]

    if snippet:
        val = re.findall(r' \d+ ', snippet)
        if len(val) > 1:
            raise Exception(f"Wtf len(val) == {len(val)} but it's supposed to be 1...")
        elif len(val) < 1:
            return None
        else:
            return val[0].strip()
    else:
        return None


async def get_verification_code(email_address: str, service=None) -> str:
    # Deprecate
    if service is None:
        service = gmail_authenticate()
    # after authentication
    # search for the confirmation email
    query = f"from:verify_at_x.com_{email_address}"
    code = None
    for i in range(10):
        emails = search_messages(service=service, query=query)
        if code:
            break
        if len(emails) > 0:
            last_email = emails[0]  # get the last one
            code = extract_verification_code(service=service, message=last_email)
        else:
            await asyncio.sleep(5)
    if not code:
        raise ValueError("No verification code found")
    else:
        return code


def extract_verification_code_imap(msg: str) -> str | None:
    code = re.findall(r'\d+', msg)
    if len(code) == 1:
        return code[0].strip()
    else:
        return None

def extract_confirmation_code_imap_new(msg: str) -> str | None:
    code = msg.split(" ")[-1]
    return code.strip().strip(".")

def extract_confirmation_code_imap_old(msg: str) -> str | None:
    code = re.findall(r"\n\d+\r", msg)[0]
    return code.strip().strip(".")

async def get_verification_code_imap(email_address: str, password: str) -> str:
    imap_ssl_host = 'imap.firstmail.ltd'
    imap_ssl_port = 993
    username = email_address
    password = password

    await asyncio.sleep(5)

    messages = []
    # Get date, subject and body len of all emails from INBOX folder
    with MailBox(imap_ssl_host).login(username, password) as mailbox:
        for msg in mailbox.fetch():
            messages.append(msg)

    # get the ones with verification code
    messages = [msg for msg in messages if "@x.com" in msg.from_]

    # get the latest one
    verification_message = max(messages, key=lambda msg: msg.date)
    if "verification" in verification_message.subject:
        verification_code = extract_verification_code_imap(verification_message.subject)
    elif "confirmation" in verification_message.subject:
        verification_code = extract_confirmation_code_imap_new(verification_message.subject)
    elif "confirm your" in verification_message.subject:
        verification_code = extract_confirmation_code_imap_old(verification_message.text)
    else:
        verification_code = None

    return verification_code

async def get_emails(email_address: str, password: str) -> list:
    imap_ssl_host = 'imap.firstmail.ltd'
    imap_ssl_port = 993
    username = email_address
    password = password

    messages = []
    # Get date, subject and body len of all emails from INBOX folder
    with MailBox(imap_ssl_host).login(username, password) as mailbox:
        for msg in mailbox.fetch():
            messages.append(msg)
    return messages


# for testing purposes
if __name__ == '__main__':
    emails = asyncio.run(
        get_emails(
            'dvaqnbdo@sabesmail.com',
            'abtrwmpgX5520')
    )
    for e in emails:
        print(e.date, e.from_, e.subject)
        print(e.text)
        print("----------")