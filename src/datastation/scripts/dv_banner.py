import argparse
import json
import logging

import requests
from rich import print_json

from datastation.config import init


def add_message(args, server_url, headers, unblock):
    headers['Content-type'] = 'application/json'  # side effect!
    url = f'{server_url}?{unblock}'
    logging.debug(f'{url} {headers}')
    data = {
        "dismissibleByUser": "true",  # list does not return this value
        "messageTexts": [
            {
                # when using another language, list returns an empty message
                # when using multiple languages 'Accept-Language' does not seem to influence list_messages
                "lang": "en",
                "message": args.message
            }
        ]
    }
    response = requests.post(url, data=json.dumps(data), headers=headers)
    print(response.content)
    response.raise_for_status()


def remove_message(args, server_url, headers, unblock):
    for msg_id in args.ids:
        url = f'{server_url}/{msg_id}?{unblock}'
        logging.debug(f'{url} {headers}')
        response = requests.delete(url, headers=headers)
        print(response.content)
        response.raise_for_status()


def list_messages(args, server_url, headers, unblock):
    headers['Content-type'] = 'application/json'  # side effect!
    url = f'{server_url}?{unblock}'
    logging.debug(f'{url} {headers}')
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    print(print_json(json.dumps(response.json()["data"], indent=4)))


def main():
    config = init()

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_add = subparsers.add_parser('add', help="Add a Banner Message")
    parser_add.add_argument('message', help="Message to add as Banner, note that HTML can be included.")
    parser_add.set_defaults(func=add_message)

    parser_remove = subparsers.add_parser('remove', help="Remove a Banner Message by its id")
    parser_remove.add_argument('ids', help="one or more ids of the Banner Message", nargs='+')
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
