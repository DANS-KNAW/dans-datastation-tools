import argparse
import logging

from datastation.config import init


def add_message(args, server_url, api_token):
    msg = args.message
    logging.debug(f'{msg} {server_url} {api_token}')


def remove_message(args, server_url, api_token):
    msg_id = args.id
    logging.debug(f'{msg_id} {server_url} {api_token}')


def list_messages(args, server_url, api_token):
    logging.debug(f'{server_url} {api_token}')


def main():
    config = init()

    server_url = config['dataverse']['server_url']
    api_token = config['dataverse']['api_token']

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_add = subparsers.add_parser('add', help="Add a Banner Message")
    parser_add.add_argument('message', help="Message to add as Banner")
    parser_add.set_defaults(func=add_message)

    parser_remove = subparsers.add_parser('remove', help="Remove a Banner Message by its id")
    parser_remove.add_argument('id', help="id of the Banner Message")
    parser_remove.set_defaults(func=remove_message)

    parser_list = subparsers.add_parser('list', help="Get a list of active Banner Messages")
    parser_list.set_defaults(func=list_messages)

    args = parser.parse_args()
    logging.info(args)
    args.func(args, server_url, api_token)


if __name__ == '__main__':
    main()
