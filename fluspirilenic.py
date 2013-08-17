import argparse
import imaplib
import logging
from ConfigParser import SafeConfigParser


logger = logging.getLogger(__name__)


def run():
    parser = argparse.ArgumentParser(description='Fluspirinelic: an IMAP toolbox')
    parser.add_argument('--src', help="The source account: <server>/<user>/<pwd>")
    parser.add_argument('--dest', help="The destination account: <server>/<user>/<pwd>")
    parser.add_argument('--ssl', action='store_true', default=False, help="Use SSL connection")
    parser.add_argument('--config', help="Points to a config file that defines the source and destination")
    parser.add_argument('--map', help="A file mapping the mailboxes to process. By default it will process both inboxes.")
    parser.add_argument('--debug', action='store_true', default=False, help="Show debug logging")
    parser.add_argument('--log', help="Log file to use")
    parser.add_argument('--limit', default=-1, type=int, help="The maxiumum number of messages to process")
    parser.add_argument('--min_uid', default=-1, type=int, help="The minimum UID to start from")
    subparsers = parser.add_subparsers()

    # List mailboxes
    parser_list = subparsers.add_parser('list', help="Lists mailboxes")
    parser_list.add_argument('--use_dest', action='store_true', default=False, help="Use the destination account instead of the source one")
    parser_list.set_defaults(func=list_mboxes)

    # Sync read/unread statuses
    parser_read = subparsers.add_parser('read', help="Synchronize the read status of messages")
    parser_read.add_argument(
            '--mode',
            choices=['read', 'unread'],
            default='unread',
            help="What to sync:\n`read` means read messages on source will be flagged read on destination.\n`unread` means unread messages on source will be flagged unread on destination.")
    parser_read.set_defaults(func=sync_read)

    # Move messages
    parser_move = subparsers.add_parser('move', help="Move messages from one mailbox to another based on another server's mailbox")
    parser_move.add_argument('--ref_mbox', help="The reference mailbox")
    parser_move.add_argument('--from_mbox', nargs='+', help="The mailbox where messages will be taken from")
    parser_move.add_argument('--to_mbox', help="The mailbox where messages will be moved to")
    parser_move.set_defaults(func=move_msgs)

    # Count messages
    parser_count = subparsers.add_parser('count', help="Counts messages")
    parser_count.add_argument('--use_dest', action="store_true", default=False, help="Use the destination account instead of the source one")
    parser_count.add_argument('--mbox', help="The source mailbox")
    parser_count.add_argument('--flags', help="The flags to filter with")
    parser_count.set_defaults(func=count_flags)
    
    args = parser.parse_args()

    log_level = logging.INFO
    if args.debug:
        log_level = logging.DEBUG
    log_file = None
    if args.log:
        log_file = args.log
    logging.basicConfig(level=log_level, filename=log_file)

    args.func(args)


def list_mboxes(args):
    src, dst = _connect(args, not args.use_dest, args.use_dest)
    acc = dst if args.use_dest else src
    if acc:
        try:
            data = acc.list()
            for mbox in data[1]:
                logger.info(mbox.split('"')[-2])
        except Exception as ex:
            logger.error(ex)
        finally:
            _disconnect(acc)


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

            try:
                dst_msgids_all = {}
                for m in mbox[1]:
                    logger.debug("Getting destination message IDs (%s)..." % m)
                    dst.select(m)
                    dst_msgids = _get_message_ids(dst)
                    dst_msgids_all[m] = dst_msgids
                    logger.debug("...got %s messages." % len(dst_msgids))

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

                    for m, msgids in dst_msgids_all.iteritems():
                        dstnum = msgids.get(val)
                        if dstnum is not None:
                            dst.select(m)
                            break
                    else:
                        logger.warning("Ignoring message because we can't find it on the destination:")
                        logger.warning(val)
                        typ, data = src.fetch(num, '(BODY.PEEK[HEADER])')
                        logger.warning(data[0])
                        logger.warning("-----------")
                        continue

                    logger.debug("  Syncing status for %s -> %s [%s] [%s \\Seen]" % (num, dstnum, val, store_action))
                    dst.store(dstnum, store_action, '\\Seen')

                    if args.limit > 0 and i >= args.limit:
                        logger.info("Reached limit of %d." % args.limit)
                        break

                    if float(i) / 100.0 == int(i / 100):
                        logger.info("Synced %d messages so far" % i)
            except Exception as ex:
                logger.error(ex)
            finally:
                _close(dst)
    except Exception as ex:
        logger.error(ex)
    finally:
        _disconnect(src, dst)


def move_msgs(args):
    if not args.ref_mbox or not args.from_mbox or not args.to_mbox:
        raise Exception("You must specify the reference mailbox, from mailbox, and to mailbox")
    src, dst = _connect(args)
    try:
        logger.debug("Getting source messages from %s" % args.ref_mbox)
        src.select(args.ref_mbox)
        ref_msgids = _get_message_ids(src)
        logger.debug("...got %d messages" % len(ref_msgids))

        from_msgids_all = {}
        for mbox in args.from_mbox:
            logger.debug("Getting destination messages from %s" % mbox)
            dst.select(mbox)
            from_msgids = _get_message_ids(dst)
            logger.debug("...got %d messages" % len(from_msgids))
            from_msgids_all[mbox] = from_msgids

        i = 0
        for ref_mid, ref_uid in ref_msgids.iteritems():
            for mbox, msgids in from_msgids_all.iteritems():
                from_uid = msgids.get(ref_mid)
                if from_uid is not None:
                    dst.select(mbox)
                    break
            else:
                logger.warning("Ignoring message because we can't find it on the destination:")
                logger.warning(ref_mid)
                typ, data = src.fetch(ref_uid, '(BODY.PEEK[HEADER])')
                logger.warning(data[0])
                logger.warning("-----------")
                continue

            logger.debug("  Moving %s" % ref_mid)
            dst.copy(from_uid, args.to_mbox)
            dst.store(from_uid, '+FLAGS.SILENT', '\\Deleted')

            i += 1
            if args.limit > 0 and i >= args.limit:
                logger.info("Reached limit of %d." % args.limit)
                break

            if float(i) / 100.0 == int(i / 100):
                logger.info("Processed %d messages so far" % i)
        dst.expunge()
        dst.close()
    except Exception as ex:
        logger.error(ex)
    finally:
        _disconnect(src, dst)


def count_flags(args):
    src, dst = _connect(args, not args.use_dest, args.use_dest)
    acc = dst if args.use_dest else src
    if acc:
        try:
            mbox = args.mbox or 'INBOX'
            acc.select(mbox)
            flags = args.flags or 'ALL'
            typ, data = acc.search(None, flags)
            msgnums = data[0].split()
            logger.info("Found %d messages in %s (searched for %s)" % (len(msgnums), mbox, flags))
        except Exception as ex:
            logger.error(ex)
        finally:
            _disconnect(acc)


def _connect(args, connect_src=True, connect_dst=True):
    if args.config:
        c = SafeConfigParser()
        c.read(args.config)
        if not args.src:
            srv = c.get('source', 'server')
            usr = c.get('source', 'username')
            pwd = c.get('source', 'password')
            args.src = "%s/%s/%s" % (srv, usr, pwd)
        if not args.dest:
            srv = c.get('destination', 'server')
            usr = c.get('destination', 'username')
            pwd = c.get('destination', 'password')
            args.dest = "%s/%s/%s" % (srv, usr, pwd)
        if c.get('options', 'ssl'):
            args.ssl = True

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
        mapping = map(lambda s: s.strip(), line.split(','))
        if len(mapping) > 1:
            mboxes.append((mapping[0], mapping[1:]))
        else:
            mboxes.append((mapping[0], [mapping[0]]))
        logger.debug("  %s -> %s" % (mboxes[-1][0], mboxes[-1][1]))
    return mboxes


def _get_message_ids(box):
    typ, data = box.fetch("1:*", "(BODY.PEEK[HEADER.FIELDS (Message-Id)])")
    msgids = {}
    for d in data:
        if len(d) == 1:
            continue
        parts = d[0].split(' ', 2)
        uid = parts[0]
        hd, msgid = _get_header(d[1])
        msgids[msgid] = uid
    return msgids


def _close(src, dst=None):
    src.close()
    if dst:
        dst.close()


def _disconnect(src, dst=None):
    src.logout()
    if dst:
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

