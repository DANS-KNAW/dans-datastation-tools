import argparse
import json
import logging

import requests

from datastation.config import init


def add_message(args, server_url, headers, unblock):
    headers['Content-type'] = 'application/json'  # side effect!
    url = f'{server_url}?{unblock}'
    logging.debug(f'{url} {headers}')
    data = {
        "dismissibleByUser": "true",  # list does not return this value
        "messageTexts": [
            {
                "lang": "en",  # when using other values, list returns an empty message
                "message": args.message
            }
        ]
    }
    response = requests.post(url, data=json.dumps(data), headers=headers)
    print(response.content)
    response.raise_for_status()


def remove_message(args, server_url, headers, unblock_query):
    url = f'{server_url}/{args.id}?{unblock_query}'
    logging.debug(f'{url} {headers}')
    response = requests.delete(url, headers=headers)
    print(response.content)
    response.raise_for_status()


def list_messages(args, server_url, headers, unblock_query):
    headers['Content-type'] = 'application/json'  # side effect!
    url = f'{server_url}?{unblock_query}'
    logging.debug(f'{url} {headers}')
    response = requests.get(url, headers=headers)
    print(json.dumps(response.json()["data"], indent=2))
    response.raise_for_status()


def main():
    config = init()

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_add = subparsers.add_parser('add', help="Add a Banner Message")
    # parser_add.add_argument('-l', '--lang', dest='lang', help="Language for the added message", default='en')
    # the following optional boolean seems to conflict with a positional argument
    # parser_add.add_argument('-d', '--dismissible', dest='dismissible', default=False, type=bool,
    #                         help="False: a user can dismiss the message only for the duration of a session. "
    #                              "True: the message can be dismissed permanently. "
    #                         )
    parser_add.add_argument('message', help="Message to add as Banner")
    parser_add.set_defaults(func=add_message)

    parser_remove = subparsers.add_parser('remove', help="Remove a Banner Message by its id")
    parser_remove.add_argument('id', help="id of the Banner Message")
    parser_remove.set_defaults(func=remove_message)

    parser_list = subparsers.add_parser('list', help="Get a list of active Banner Messages")
    parser_list.set_defaults(func=list_messages)

    args = parser.parse_args()
    logging.info(args)

    cfg = config['dataverse']
    headers = {'X-Dataverse-key': cfg["api_token"]}
    unblock = f'unblock-key={cfg["unblock-key"]}'

    args.func(args, f'{cfg["server_url"]}/api/admin/bannerMessage', headers, unblock)


if __name__ == '__main__':
    main()
