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
    parser.add_argument('--min_uid', default=-1, type=int, help="The minimum UID to start from")
    subparsers = parser.add_subparsers()

    parser_read = subparsers.add_parser('read', help="Synchronize the read status of messages")
    parser_read.add_argument(
            '--mode',
            choices=['read', 'unread'],
            default='unread',
            help="What to sync:\n`read` means read messages on source will be flagged read on destination.\n`unread` means unread messages on source will be flagged unread on destination.")
    parser_read.set_defaults(func=sync_read)

    parser_count = subparsers.add_parser('count', help="Counts messages with some flags")
    parser_count.add_argument('--src_mbox', help="The source mailbox")
    parser_count.add_argument('--dst_mbox', help="The destination mailbox")
    parser_count.add_argument(
            '--flags',
            help="The flags to search for")
    parser_count.set_defaults(func=count_flags)
    
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
                    if args.min_uid > 0 and int(num) < args.min_uid:
                        continue

                    typ, data = src.fetch(num, '(UID FLAGS BODY.PEEK[HEADER.FIELDS (Message-ID)])')
                    hd, val = _get_header(data[0][1])
                    if hd.lower() != 'Message-ID'.lower():
                        raise Exception("Expected Message-ID, got %s" % hd)
                    if not val or val == "":
                        logger.warning("Ignoring message because it doesn't have a Message-ID:")
                        typ, data = src.fetch(num, '(BODY.PEEK[HEADER])')
                        logger.warning(data[0])
                        continue

                    try:
                        typ, data = dst.search(None, 'HEADER', 'Message-ID', '"%s"' % val)
                        dstnum = data[0]
                    except Exception as err:
                        logger.warning("Ignoring message because search on destination failed:")
                        logger.warning(err)
                        typ, data = src.fetch(num, '(BODY.PEEK[HEADER])')
                        logger.warning(data[0])
                        continue

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


def count_flags(args):
    src, dst = _connect(args, bool(args.src), bool(args.dest))
    try:
        if src:
            src.select(args.src_mbox or 'INBOX')
            typ, data = src.search(None, args.flags)
            msgnums = data[0].split()
            logger.info("Found %d messages in source with flags %s" % (len(msgnums), args.flags))
        if dst:
            dst.select(args.dst_mbox or 'INBOX')
            typ, data = dst.search(None, args.flags)
            msgnums = data[0].split()
            logger.info("Found %d messages in destination with flags %s" % (len(msgnums), args.flags))
    finally:
        _disconnect(src, dst)


def _connect(args, connect_src=True, connect_dst=True):
    if connect_src and not args.src:
        raise Exception("You must specify a source")
    if connect_dst and not args.dest:
        raise Exception("You must specify a destination.")

    IMAP = imaplib.IMAP4
    if args.ssl:
        IMAP = imaplib.IMAP4_SSL

    if connect_src:
        src_server, src_user, src_pwd = _get_credentials(args.src)
        logger.info("Connecting to %s" % src_server)
        src = IMAP(src_server)
        src.login(src_user, src_pwd)
    else:
        src = None

    if connect_dst:
        dst_server, dst_user, dst_pwd = _get_credentials(args.dest)
        logger.info("Connecting to %s" % dst_server)
        dst = IMAP(dst_server)
        dst.login(dst_user, dst_pwd)
    else:
        dst = None
    
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

