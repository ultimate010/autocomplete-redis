#-*- coding:utf-8 -*-
import redis
import simplejson
import jieba
import logging
from pypinyin import pinyin
import pypinyin

class Autocomplete (object):
  """
  autocomplete.
  """

  def __init__ (self, scope, redisaddr="localhost", port=6379,
                db=0, limits=5, cached=True):
    self.r = redis.Redis (redisaddr, port=port, db=db)
    self.scope = scope
    self.cached=cached
    self.limits = limits
    self.database = "database:%s" % scope
    self.indexbase = "indexbase:%s" % scope

  def _get_index_key (self, key):
    return "%s:%s" % (self.indexbase, key)

  def del_index (self):
    prefixs = self.r.smembers (self.indexbase)
    for prefix in prefixs:
      self.r.delete(self._get_index_key(prefix))
    self.r.delete(self.indexbase)
    self.r.delete(self.database)

  def sanity_check (self, item):
    """
    Make sure item has key that's needed.
    """
    for key in ("uid","term"):
      if not item.has_key (key):
        raise Exception ("Item should have key %s"%key )

  def add_item (self,item):
    """
    Create index for ITEM.
    """
    self.sanity_check (item)
    self.r.hset (self.database, item.get('uid'), simplejson.dumps(item))
    for prefix in self.prefixs_for_term (item['term']):
      self.r.sadd (self.indexbase, prefix)
      self.r.zadd (self._get_index_key(prefix),item.get('uid'), item.get('score',0))

  def del_item (self,item):
    """
    Delete ITEM from the index
    """
    for prefix in self.prefixs_for_term (item['term']):
      self.r.zrem (self._get_index_key(prefix), item.get('uid'))
      if not self.r.zcard (self._get_index_key(prefix)):
        self.r.delete (self._get_index_key(prefix))
        self.r.srem (self.indexbase, prefix)

  def update_item (self, item):
    self.del_item (item)
    self.add_item (item)

  def prefixs_for_term (self,term):
    """
    Get prefixs for TERM.
    """
    # Normalization
    term=term.lower()

    # Prefixs for term
    prefixs=[]
    for i in xrange(1, len(term) + 1):
      word = term[:i]
      prefixs.append(word)
      prefixs.append(''.join([i[0] for i in pinyin(word, style=pypinyin.FIRST_LETTER)]).lower())
      prefixs.append(''.join([i[0] for i in pinyin(word, style=pypinyin.NORMAL)]).lower())
      prefixs.append(word)

    tokens = self.normalize(term)
    for token in tokens:
      for i in xrange (1,len(token)+1):
        word = token[:i]
        prefixs.append(word)
        prefixs.append(''.join([i[0] for i in pinyin(word, style=pypinyin.INITIALS)]).lower())
        prefixs.append(''.join([i[0] for i in pinyin(word, style=pypinyin.NORMAL)]).lower())
        prefixs.append(word)

    return list(set(prefixs))

  def normalize (self,prefix):
    """
    Normalize the search string.
    """
    return list(set([token for token
                     in jieba.cut(prefix.lower()) if token != " "]))

  def search_query (self,prefix):
    if not isinstance(prefix, unicode):
        prefix = unicode(prefix)
    search_strings = self.normalize (prefix)

    if not search_strings: return []

    cache_key = self._get_index_key (('|').join(search_strings))

    if not self.cached or not self.r.exists (cache_key):
      self.r.zinterstore (cache_key, map (lambda x: self._get_index_key(x), search_strings))
      self.r.expire (cache_key, 10 * 60)

    ids=self.r.zrevrange (cache_key, 0, self.limits)
    if not ids: return ids
    return map(lambda x:simplejson.loads(x),
               self.r.hmget(self.database, *ids))
