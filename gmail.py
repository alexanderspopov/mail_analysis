import os
import imaplib
import email
import json
import multiprocessing
import pickle
import logging
from multiprocessing.pool import ThreadPool
from collections import defaultdict

from dateutil import parser as date_parser

# number of imap connections we'll keep open concurrently while
# running a large fetch.
CONNECTION_POOL_SIZE = 10

IMAP_OK = 'OK'


logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class Gmail:

  """
  Class that wraps the undrlying imaplib interface and provides a
  slightly higher level of abstraction.
  """

  def __init__(self, host, user, password, id_):
    self.imap = imaplib.IMAP4_SSL(host)
    self.imap.login(user, password)
    self.id = id_

  def get_mailboxes(self):
    status, mailboxes = self.imap.list()
    parse_mailbox = lambda mailbox: mailbox.split('" "')[-1][:-1]
    wanted = (parse_mailbox(m) for m in mailboxes if '[Gmail]' not in m)
    return wanted

  def fetch_message_ids(self, mailbox):

    logger.info((self.id, 'fetch_message_ids', mailbox))

    status = self.imap.select(mailbox)
    status, data = self.imap.search(None, 'ALL')

    if status != IMAP_OK:
      msg = 'Failed to fetch messages for mailbox {}.'.format(mailbox)
      raise Exception(msg)

    message_nums = data[0].split()

    return mailbox, message_nums

  def fetch_message(self, mailbox, message_num):

    logger.info((self.id, 'fetch_message', mailbox, message_num))

    self.imap.select(mailbox)
    status, content = self.imap.fetch(message_num, '(RFC822)')
    if status != IMAP_OK:
      msg = ('Failed to retrieve message {} from '
             'mailbox {}.'.format(message_num, mailbox))
      raise Exception(msg)
    else:
      body = content[0][1]
      return email.message_from_string(body)


def scrape(imap_host, user, password, mailboxes=None):
  """
  Fetch all the emails from a given gmail account.

  Args:
    imap_host:  IMAP host
    user:       mail account user name
    password:   mail account password
  """

  # the IMAP module is not thread safe. that means if we want to do
  # parallel processing, we can't share a single imap connection be it
  # via threading, multiprocessing, or async io.
  # instead we create a pool of connections, and our parallel workers
  # can grab a connection from the pool when they need to do some work.
  connections = [Gmail(imap_host, user, password, i)
                 for i in range(CONNECTION_POOL_SIZE)]

  manager = multiprocessing.Manager()
  # this is a list of available connection "ids", where ids are just ints
  # 0..CONNECTION_POOL_SIZE. when a worker needs a connection, it will
  # grab the id of one of the available workers, and then use that to
  # get the connection reference itself. this was done because originally
  # we tried to use multiprocessing and the list of connections could not be
  # serialized. we switched to threads eventually, so this is not strictly
  # necessary, but it works.
  avail_connections = manager.list(range(CONNECTION_POOL_SIZE))

  lock = multiprocessing.Lock()

  def round_robin_connections(args):
    """
    Get an available connection from the connection pool and do some
    work with it.
    """
    # "selector"  is the method we want to call on the connection
    # "func_args" are the arguments we want to pass to the method 
    selector, func_args = args
    with lock:
      connection_id = avail_connections.pop()
      connection = connections[connection_id]
    res = getattr(connection, selector)(*func_args)
    with lock:
      avail_connections.append(connection_id)
    return res

  workers = min(len(connections), multiprocessing.cpu_count() * 2)
  pool = ThreadPool(workers)

  if mailboxes is None:
    mailboxes = list(connections[0].get_mailboxes())

  # get the message ids from each mailbox. we'll use these ids later to
  # actually get the message itself.
  tasks = [('fetch_message_ids', (mailbox, )) for mailbox in mailboxes]
  job = pool.map_async(round_robin_connections, tasks)
  message_ids = job.get()

  # "message_ids" looks like [[mailbox1, [id1, id2, ...]], ...]
  # we want to turn it into [[mailbox1, id1], [mailbox1, id2], ...]
  flat =  [(mailbox, id_) for mailbox, ids in message_ids for id_ in ids]
  tasks = [('fetch_message', (mailbox, id_)) for mailbox, id_ in flat]
  job = pool.map_async(round_robin_connections, tasks)
  # this will be a list of email.message.Message instances
  messages = job.get()

  return messages


if __name__ == '__main__':

  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument('host')
  parser.add_argument('username')
  parser.add_argument('password')
  parser.add_argument('--output-file', default='gmail.pickle')
  parser.add_argument('--mailboxes', nargs='+')
  parser.add_argument('--connections', type=int, default=CONNECTION_POOL_SIZE)

  args = parser.parse_args()

  messages = scrape(args.host, args.username, args.password, mailboxes=args.mailboxes)

  with open(args.output_file, 'wb') as fp:
    pickle.dump(messages, fp)
