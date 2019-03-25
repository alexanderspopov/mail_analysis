'''
Parse pickled emails and do some textual analysis on them.

Can be executed via command line or via module imports.
'''

import pickle
import re
from collections import defaultdict

from dateutil import parser 
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction import text


def normalize_email_address(addr):
  '''
  Normalize the sender address of a parsed email.

	Args:
    addr: 'From' field of an email  

  Returns:
    addr: normalized email address
  '''
  addr = addr.lower()
  extract = re.search('<([^>]+)>', addr)
  if extract:
    addr = extract.group(1)
  return addr


def parse_email(email):
  '''
  Parse the sender, date, and body of an email message.

  Args:
    email:  instance of email.message.Message

  Returns:
    from_:  email address of sender
    date:   datetime.datetime of message
    body:   string of email body
  '''

  from_ = normalize_email_address(email['From'])
  date = parser.parse(email['Date'])

  if email.is_multipart():
    payloads = email.get_payload()
  else:
    payloads = [email]

  body = None
  for payload in payloads:
    content_type = payload.get_content_type()

    if content_type != 'text/plain':
      continue

    contents = payload.get_payload().split('\r\n')
    keep_lines = []
    for line in contents:

      # truncate the forwarded parts of an email as well as the contents of the
      # previous email that the author is replying to
      if re.search('On [A-Za-z]{2,3}, [A-Za-z]{2,3} \d{1,2}, \d{4} at', line) or \
         re.search('On [A-Za-z]{2,3} \d{1,2}, \d{4}, at', line) or \
         line.startswith('Begin forwarded message:') or \
         re.search('-+ Forwarded message -+', line):
        break

      if line.strip():
        keep_lines.append(line)

    if keep_lines:
      body = '\r\n'.join(keep_lines)
      break

  return from_, date, body


def get_email_indices_for_word(word, vectorizer, model):
  '''
  Return the indices of the emails in the corpus that contain a given word.

  Args:
    word:       word for which we want indices of the emails that contain it
    vectorizer: instance of CountVectorizer
    model:      document-term matrix, output of vectorizer.fit_transform

  Returns:
    indices:    list of email indices
  '''
  idx = vectorizer.vocabulary_.get(word)
  rows = model[:, idx].T.toarray()[0]
  return [i for i, e in enumerate(rows) if e == 1]


def get_word_counts(vectorizer, model):
  '''
  Get word counts for all words in the vectorizer's vocabulary extracted
  from a corpus.

  Args:
    vectorizer:   instance of CountVectorizer
    model:        document-term matrix, output of vectorizer.fit_transform

  Returns:
    word_counts:  list of (<word>, <count>) sorted by count in descending
                  order.
  '''
  counts = model.sum(axis=0).tolist()[0]
  words = vectorizer.get_feature_names()
  word_counts = zip(words, counts)
  return sorted(word_counts, key=lambda row: row[1], reverse=True)


def analyze(emails):
  '''
  Given a list of emails, parse the emails and do some simple word analysis.

  Args:
    emails:         list of email.message.Message

  Returns:
    correspondence: dict of <from_email>: (<index of msg in emails>, <date>,
                                           <body>)
    author_words:   dict of <from_email>: (<vectorizer>, <model>, <word_counts>) 
  '''

  correspondence = defaultdict(list)
  author_words = dict()

  for i, email in enumerate(emails):
    from_, date, body = parse_email(email)
    if body is not None:
      correspondence[from_].append((i, date, body))

  for author, corpus in correspondence.iteritems():

    if len(corpus) < 10:
      continue

    try:
      vectorizer = CountVectorizer(max_features=1000, min_df=3, max_df=0.85,
                                   token_pattern=r'(?u)\b[a-zA-Z]{3,}\b',
                                   stop_words=text.ENGLISH_STOP_WORDS)
      # model is an array where each row is a document (i.e., email body) in the
      # corpus and column is the frequency of occurrence of a given feature (i.e,
      # word) in the document. the column labels can be retrieved via
      # vectorizer.get_feature_names.
      model = vectorizer.fit_transform([row[2] for row in corpus])
      model[model != 0] = 1
      word_counts = get_word_counts(vectorizer, model)
      author_words[author] = (vectorizer, model, word_counts)
    except Exception as e:
      print e

  return correspondence, author_words


def main(input_pickle_path):

  '''
  Entry point when executed from command line. Performs simple word analysis
  on a pickle dump of emails.

  Args:
    input_pickle_path:  path of pickle dump of emails

  Returns:
    correspondence:     see `analyze`
    author_words:       see `analyze`
  '''

  with open(input_pickle_path, 'rb') as fp:
    emails = pickle.load(fp)

  return analyze(emails)


if __name__ == '__main__':

  import argparse

  parser = argparse.ArgumentParser()
  parser.add_argument('input_pickle_path')

  args = parser.parse_args()

  main(args.input_pickle_path)
