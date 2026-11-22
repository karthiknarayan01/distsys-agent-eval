#!/usr/bin/env python3
"""Extract the grading-relevant metrics from Maelstrom's results.edn."""
import re, sys

path = sys.argv[1] if len(sys.argv) > 1 else 'store/latest/results.edn'
s = open(path).read()

def grab(pat):
    m = re.search(pat, s)
    return m.group(1) if m else '??'

m = re.search(r':stable-latencies \{([^}]*)\}', s)
lat = '??'
if m:
    toks = m.group(1).replace(',', ' ').split()
    d = dict(zip(toks[::2], toks[1::2]))
    lat = 'median=%s max=%s' % (d.get('0.5'), d.get('1'))

print('valid?           ', grab(r':valid? (true|false|:unknown)'))
print('servers msgs/op  ', grab(r':servers \{[^}]*?msgs-per-op ([0-9.]+)'))
print('all msgs/op      ', grab(r':all \{[^}]*?msgs-per-op ([0-9.]+)'))
print('stable latencies ', lat)
print('lost-count       ', grab(r':lost-count (\d+)'))
print('never-read-count ', grab(r':never-read-count (\d+)'))
print('attempt-count    ', grab(r':attempt-count (\d+)'))
print('server msgs sent ', grab(r':servers \{:send-count (\d+)'))
