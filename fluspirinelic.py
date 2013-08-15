import argparse
import imaplib
import logging


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def run():
    parser = argparse.ArgumentParser(description='Fluspirinelic: an IMAP toolbox')
    parser.add_argument('--src', help="The source account: <server>/<user>/<pwd>")
    parser.add_argument('--dest', help="The destination account: <server>/<user>/<pwd>")
    parser.add_argument('--ssl', action='store_true', default=False, help="Use SSL connection")
    parser.add_argument('--map', help="A file mapping the mailboxes to process. By default it will process both inboxes.")
    parser.add_argument('--debug', action='store_true', default=False, help="Show debug logging")
    parser.add_argument('--limit', default=-1, type=int, help="The maxiumum number of messages to process")
    subparsers = parser.add_subparsers()
    parser_read = subparsers.add_parser('read', help="Synchronize the read status of messages")
    parser_read.add_argument(
            '--mode',
            choices=['read', 'unread'],
            default='unread',
            help="What to sync:\n`read` means read messages on source will be flagged read on destination.\n`unread` means unread messages on source will be flagged unread on destination.")
    parser_read.set_defaults(func=sync_read)
    
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
    args.func(args)


def sync_read(args):
    mboxes = _get_mailboxes(args)
    src, dst = _connect(args)

    if args.mode == 'read':
        search_flag = 'SEEN'
        store_action = '+FLAGS.SILENT'
    elif args.mode == 'unread':
        search_flag = 'UNSEEN'
        store_action = '-FLAGS.SILENT'
    else:
        raise Exception("Unsupported mode: %s" % args.mode)

    try:
        for mbox in mboxes:
            logger.info("Syncing %s -> %s" % (mbox[0], mbox[1]))
            src.select(mbox[0])
            dst.select(mbox[1])

            try:
                logger.debug("Searching for messages with flag '%s'..." % search_flag)
                typ, data = src.search(None, search_flag)
                msgnums = data[0].split()
                total = len(msgnums)
                logger.debug("...got %d messages." % total)

                for i, num in enumerate(msgnums):
                    typ, data = src.fetch(num, '(UID FLAGS BODY.PEEK[HEADER.FIELDS (Message-ID)])')
                    hd, val = _get_header(data[0][1])
                    if hd != 'Message-ID':
                        raise Exception("Expected Message-ID, got %s" % hd)

                    typ, data = dst.search(None, 'HEADER', 'Message-ID', '"%s"' % val)
                    dstnum = data[0]

                    logger.debug("  Syncing status for %s -> %s [%s] [%s \\Seen]" % (num, dstnum, val, store_action))
                    dst.store(dstnum, store_action, '\\Seen')

                    if args.limit > 0 and i >= args.limit:
                        logger.info("Reached limit of %d." % args.limit)
                        break

                    if float(i) / 100.0 == int(i / 100):
                        logger.info("Synced %d messages so far" % i)
            finally:
                _close(src, dst)
    finally:
        _disconnect(src, dst)


def _connect(args):
    if not args.src or not args.dest:
        raise Exception("You must specify a source and a destination.")

    src_server, src_user, src_pwd = _get_credentials(args.src)
    dst_server, dst_user, dst_pwd = _get_credentials(args.dest)
    IMAP = imaplib.IMAP4
    if args.ssl:
        IMAP = imaplib.IMAP4_SSL

    logger.info("Connecting to %s" % src_server)
    src = IMAP(src_server)
    src.login(src_user, src_pwd)
    logger.info("Connecting to %s" % dst_server)
    dst = IMAP(dst_server)
    dst.login(dst_user, dst_pwd)
    
    return src, dst


def _get_mailboxes(args):
    if not args.map:
        return [('INBOX', 'INBOX')]
    logger.debug("Loading mailbox mapping file: %s" % args.map)
    with open(args.map) as f:
        lines = f.readlines()
    mboxes = []
    for line in lines:
        if not line.strip():
            continue
        mapping = map(lambda s: s.strip(), line.split(',', 2))
        if len(mapping) > 1:
            mboxes.append(tuple(mapping))
        else:
            mboxes.append((mapping[0], mapping[0]))
        logger.debug("  %s -> %s" % (mboxes[-1][0], mboxes[-1][1]))
    return mboxes


def _close(src, dst):
    src.close()
    dst.close()


def _disconnect(src, dst):
    src.logout()
    dst.logout()


def _get_credentials(creds):
    items = creds.split('/')
    if len(items) != 3:
        raise Exception("Invalid credentials: %s" % creds)
    return tuple(items)


def _get_header(header):
    values = header.split(':', 2)
    return (values[0].strip(), values[1].strip())


if __name__ == '__main__':
    run()

