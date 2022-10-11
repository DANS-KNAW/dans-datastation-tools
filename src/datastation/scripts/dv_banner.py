import argparse
import json
import logging

import requests
import rich

from datastation.config import init


def add_message(args, banner_url, headers, unblock):
    headers['Content-type'] = 'application/json'  # side effect!
    data = json.dumps({
        "dismissibleByUser": "true",  # list does not return this value
        "messageTexts": [
            {
                # when using another language, list returns an empty message
                # when using multiple languages, 'Accept-Language' on list_messages does not make a difference
                "lang": "en",
                "message": args.message
            }
        ]
    })
    response = requests.post(f'{banner_url}?{unblock}', data=data, headers=headers)
    print(response.content)
    response.raise_for_status()


def remove_message(args, banner_url, headers, unblock):
    for msg_id in args.ids:
        response = requests.delete(f'{banner_url}/{msg_id}?{unblock}', headers=headers)
        print(response.content)
        response.raise_for_status()


def list_messages(args, banner_url, headers, unblock):
    headers['Content-type'] = 'application/json'  # side effect!
    response = requests.get(f'{banner_url}?{unblock}', headers=headers)
    response.raise_for_status()
    for row in response.json()["data"]:
        rich.print(row)


def main():
    config = init()

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_add = subparsers.add_parser('add', help="Add a Banner Message")
    parser_add.add_argument('message', help="Message to add as Banner, note that HTML can be included.")
    parser_add.set_defaults(func=add_message)

    parser_remove = subparsers.add_parser('remove', help="Remove Banner Message by their id-s")
    parser_remove.add_argument('ids', help="One or more ids of the Banner Message", nargs='+')
    parser_remove.set_defaults(func=remove_message)

    parser_list = subparsers.add_parser('list', help="Get a list of active Banner Messages")
    parser_list.set_defaults(func=list_messages)

    args = parser.parse_args()
    logging.info(args)

    cfg = config['dataverse']
    headers = {'X-Dataverse-key': cfg["api_token"]}
    unblock = f'unblock-key={cfg["unblock-key"]}'
    banner_url = f'{cfg["server_url"]}/api/admin/bannerMessage'

    args.func(args, banner_url, headers, unblock)


if __name__ == '__main__':
    main()
