# -*- coding: utf-8 -*-
import logging
import os
import oss2
import json
import time


# Copy multiple objects specified by keys from src_bucket to dest_bucket.

# event format
# {
#   "src_bucket": "",
#   "dest_bucket": "",
#   "key": "a"
# }

# 业务需求:
# 从event中读取dest_oss_endpoint
# 从源文件object中读取object的各项属性: filetype, mediaid, mediatype, localvideopath, localcoverpath...
# 获取vod_client
# 设置vod上传的各项参数: appId, filetype, mediaId, mediaType, localVideoPath, localCoverPath, ossObjectName...
# vod_client上传, 文档地址: https://help.aliyun.com/document_detail/64148.html?spm=a2c4g.11186623.2.20.245cce9dJLR3wC

def handler(event, context):
  logger = logging.getLogger()
  # json转化
  evt = json.loads(event)
  logger.info("Handling event: %s", evt)
  # os.environ['SRC_OSS_ENDPOINT']读取template.yml配置文件, 获取oss bucket endpoint
  # evt["dest_bucket"] 从event中获取源bucket name
  src_client = get_oss_client(context, os.environ['SRC_OSS_ENDPOINT'], evt["src_bucket"])
  # evt.get("dest_oss_endpoint") or os.environ['DEST_OSS_ENDPOINT'] 生产环境中从event中获取目标bucket地址, 测试时从配置文件中读取
  dest_client = get_oss_client(context, evt.get("dest_oss_endpoint") or os.environ['DEST_OSS_ENDPOINT'], evt["dest_bucket"])
  # evt["key"] 从指定bucket中获取文件
  copy(src_client, dest_client, evt["key"])

  return {}

#
def copy(src_client, dest_client, key):
  logger = logging.getLogger()
  logger.info("Starting to copy %s", key)
  start_time = time.time()
  # 获取源文件流
  object_stream = src_client.get_object(key)
  # 调用阿里client上传文件
  res = dest_client.put_object(key, object_stream)
  end_time = time.time()
  logger.info('Copied %s in %s secs', key, end_time-start_time)

# 根据当前用户context, oss endpoint, bucket name获取oss client
def get_oss_client(context, endpoint, bucket):
  creds = context.credentials
  if creds.security_token != None:
    auth = oss2.StsAuth(creds.access_key_id, creds.access_key_secret, creds.security_token)
  else:
    # for local testing, use the public endpoint
    endpoint = str.replace(endpoint, "-internal", "")
    auth = oss2.Auth(creds.access_key_id, creds.access_key_secret)
  return oss2.Bucket(auth, endpoint, bucket)